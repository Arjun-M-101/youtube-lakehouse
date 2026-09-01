# 📊 YouTube Lakehouse — AWS Batch Data Engineering Pipeline

## 🚀 Overview

This is the AWS-native rebuild of my earlier local YouTube data engineering project (Airflow + Spark + Postgres + Streamlit — see [that repo](#) for the local version). Where the local version proved the Medallion Architecture on a single machine, this version proves the same design as real managed cloud infrastructure: event-driven ingestion, a serverless orchestrator, distributed Spark ETL on AWS Glue, an explicit data-quality gate, a private serverless warehouse, and a BI layer — all defined as code and torn down when not in use.

The first AWS account I tried this on turned out to be a dead end — Redshift and Glue were both blocked at the account level, not the IAM level (see [Production Problem #1](#-production-problems-i-hit-and-how-i-fixed-them) below). This repo is the clean rebuild on a working account.

It ingests the [Kaggle "YouTube Trending Video" dataset](https://www.kaggle.com/datasets/datasnaek/youtube-new) (multi-region CSV exports: US, GB, IN, and optionally CA/DE/FR/etc.), cleans and validates it, quarantines anything that doesn't meet quality rules, aggregates it into daily category/region summaries, loads it into a warehouse, and publishes a QuickSight dashboard on top.

## 🏗️ Architecture

```
                 S3 ObjectCreated (bronze/youtube/*.csv)
                              │
                              ▼
                       Lambda (trigger_pipeline)
                    thin — only starts Step Functions
                              │
                              ▼
                 ┌─────────────────────────────┐
                 │   Step Functions state machine │
                 │  youtube-lakehouse-batch-pipeline │
                 └─────────────────────────────┘
                              │
                 ┌────────────┴────────────┐
                 ▼                          
        Glue: Bronze → Silver               
   (validate, clean, dedupe, quarantine,     
    write DQ report to S3)                   
                 │
                 ▼
        Read DQ report  ──► DataQualityGate (Choice state)
                 │                       │
            pass_rate ≥ threshold   pass_rate < threshold
                 │                       │
                 ▼                       ▼
        Glue Crawler (Silver)     SNS: NotifyDataQualityFailure
                 │                       │
                 ▼                       ▼
        Glue: Silver → Gold        Fail state: PipelineFailed
     (aggregate, load Redshift
      via JDBC, truncate+insert)
                 │
                 ▼
        Redshift Serverless (private VPC, 3 AZs)
                 │
        ┌────────┴─────────┐
        ▼                  ▼
     Athena           QuickSight (VPC-connected)
  (ad hoc Silver        dashboard on Gold
   detail queries)
```

A daily **EventBridge Scheduler** run also fires the same state machine with an empty `triggeredKey`, so Bronze → Silver reprocesses the full Bronze prefix as a backstop even if no new file lands that day.

## ✅ Every AWS service this project actually uses

Confirmed directly against the Terraform source — nothing here is aspirational, all of it is provisioned:

| Service | Resource(s) in this repo | Role |
|---|---|---|
| **S3** | `s3.tf` | Bronze/Silver/Gold/Quarantine/DQ-reports data lake, versioned, encrypted, public access blocked |
| **Lambda** | `lambda.tf` | Thin S3-event trigger — starts Step Functions, does no data processing |
| **Step Functions** | `step_functions.tf`, `step_functions/state_machine.json` | Orchestration: DQ branching, crawler wait/retry, success/failure handling |
| **Glue (Spark ETL)** | `glue.tf` — `bronze-to-silver`, `silver-to-gold` jobs (Glue 5.1, G.1X workers) | Distributed transform + aggregate |
| **Glue Data Catalog + Crawler** | `glue.tf` — `silver-crawler` | Schema discovery for Silver Parquet |
| **Redshift Serverless** | `redshift.tf` — private, 3-AZ, `publicly_accessible = false` | Gold warehouse (`gold.category_daily_summary`) |
| **Athena** | `athena.tf` — `youtube-lakehouse-detail` workgroup, 2 saved queries | Ad-hoc per-video Silver queries |
| **QuickSight** | `quicksight.tf` — VPC connection, Redshift data source, `category-daily-performance` dataset | BI dashboard on Gold |
| **SNS** | `sns.tf` | Pipeline/DQ failure alerts |
| **CloudWatch** | via Glue/Lambda logging flags | Logs + metrics |
| **EventBridge Scheduler** | `eventbridge.tf` | Daily backstop trigger |
| **Secrets Manager** | `iam.tf` — `youtube-lakehouse-youtube-data-api-key`, `youtube-lakehouse-redshift-credentials` | YouTube API key + Redshift credentials |
| **dbt** | `dbt_project/` | Generic + business-rule tests on the warehouse |
| **GitHub Actions** | `.github/workflows/ci.yml` | pytest + `terraform fmt`/`validate` + `dbt parse` on every push |

If you're wondering "do I need S3? Redshift? Athena?" — yes, all of the above, no more and no less. There is nothing else to add.

## 📂 Project Structure

```
youtube-lakehouse/
│
├── src/
│   ├── glue_jobs/
│   │   ├── bronze_to_silver.py     # Validate, clean, dedupe, DQ report, quarantine
│   │   └── silver_to_gold.py       # Aggregate + load into Redshift (JDBC)
│   ├── lambda/
│   │   └── trigger_pipeline.py     # Thin S3-event → Step Functions starter
│   ├── transform_logic.py          # Pure, unit-testable validation/aggregation logic
│   ├── api_client.py               # YouTube Data API v3 client (retry/backoff)
│   └── category_enrichment.py      # Parses category API responses
│
├── step_functions/
│   └── state_machine.json          # Orchestrator definition (source of truth)
│
├── terraform/                      # All infrastructure as code
│   ├── s3.tf, networking.tf, iam.tf
│   ├── lambda.tf, glue.tf, step_functions.tf, eventbridge.tf
│   ├── redshift.tf, athena.tf, quicksight.tf, sns.tf
│   └── terraform.tfvars.example    # Template — copy, never commit the real file
│
├── dbt_project/                    # Warehouse-side tests on Gold
│
├── tests/                          # pytest — runs with no AWS account needed
│   ├── test_transform_logic.py
│   ├── test_api_client.py
│   ├── test_category_enrichment.py
│   └── test_trigger_pipeline.py
│
├── reference/
│   └── youtube_categories.json     # Fallback category reference
│
├── sample_data/                    # Kaggle CSVs go here (gitignored — see below)
│
├── docs/
│   ├── ARCHITECTURE.md
│   └── PRODUCTION_DEPLOYMENT.md
│
├── screenshots/                    # Dashboard + pipeline proof (see below)
├── .github/workflows/              # CI: pytest + terraform fmt/validate + dbt parse
├── requirements.txt / requirements-dbt.txt
└── Makefile
```

## 🛠️ Prerequisites

- AWS account with billing alert configured (this project is designed to run for a few dollars and be torn down — see [Teardown](#-teardown--cost-control))
- AWS CLI v2, configured with credentials that can create IAM/S3/Glue/Redshift/QuickSight/Step Functions resources
- Terraform ≥ 1.5
- Python 3.11+ (matches Glue 5.1's Python runtime)
- A YouTube Data API v3 key ([console.cloud.google.com](https://console.cloud.google.com) → enable "YouTube Data API v3" → create credentials)
- A QuickSight account (Standard, free trial is fine) and your QuickSight user ARN
- Git and a GitHub account (see the push section below if this is your first time)

## 🔀 First time: push this project to GitHub

Do this once, before or after deploying — it's independent of AWS.

```bash
cd youtube-lakehouse
git init
git add .
git status                     # confirm no .tfstate, .venv/, tfvars, or credentials are staged — see the security section below
git commit -m "Initial commit: YouTube Lakehouse AWS data engineering project"
```

Create a new **empty** repository at [github.com/new](https://github.com/new) — name it `youtube-lakehouse`, leave "Add a README/.gitignore/license" **unchecked** since you already have all three locally. Then:

```bash
git remote add origin https://github.com/<your-username>/youtube-lakehouse.git
git branch -M main
git push -u origin main
```

If GitHub rejects your password over HTTPS, generate a Personal Access Token (GitHub → Settings → Developer settings → Personal access tokens) and use that in place of the password, or run `gh auth login` if you have the GitHub CLI installed.

Every push after this is just `git add . && git commit -m "..." && git push`.

## ⚙️ Setup Instructions

### 1. Clone and set up Python

```bash
git clone https://github.com/<your-username>/youtube-lakehouse.git
cd youtube-lakehouse
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Run the unit tests first (no AWS needed)

```bash
pytest tests/ -v
```

These exercise validation rules, region resolution, deduplication, the DQ report formula, API retry/backoff, category-response parsing, and the Lambda trigger — all without touching AWS. Green tests here catch most logic bugs before they cost you a `terraform apply`.

### 3. Configure Terraform variables

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` and fill in `alert_email` and `quicksight_user_arn`. **Never put the Redshift password in this file** — it's supplied through an environment variable instead:

```bash
export TF_VAR_redshift_admin_password='use-a-strong-password-here'
```

### 4. Deploy

```bash
terraform init
terraform validate
terraform plan
```
Read the plan before applying — always. Then:
```bash
terraform apply -var="redshift_admin_password=$TF_VAR_redshift_admin_password"
```
Confirm the SNS subscription email that lands in your inbox — pipeline failure alerts won't reach you until you do.

### 5. Store the YouTube API key in Secrets Manager

Terraform creates the secret container; you populate the value (never in code, never in tfvars):

```bash
aws secretsmanager put-secret-value \
  --secret-id youtube-lakehouse-youtube-data-api-key \
  --secret-string 'YOUR_YOUTUBE_API_KEY'
```

### 6. Get the data

```bash
mkdir -p sample_data
```
Download the CSVs from [Kaggle "YouTube Trending Video" dataset](https://www.kaggle.com/datasets/datasnaek/youtube-new) into `sample_data/`. **Keep the original filenames** (`USvideos.csv`, `GBvideos.csv`, `INvideos.csv`, ...) — the pipeline resolves `region` from the filename, not from file content. A renamed file intentionally fails the `UNKNOWN_REGION` rule.

### 7. Upload to trigger the pipeline

```bash
aws s3 cp sample_data/USvideos.csv s3://$(terraform output -raw lakehouse_bucket_name)/bronze/youtube/USvideos.csv
```
Each upload independently triggers Lambda → Step Functions. Watch it in the Step Functions console (`youtube-lakehouse-batch-pipeline`) until it reaches `SUCCEEDED`. Repeat for GB, IN, or any other region file you want loaded — each run is a full, safe recompute (see [Data Flow](#-data-flow) below for why that's safe).

### 8. Verify the data landed

Redshift (Gold table row counts):
```bash
SECRET_ARN=$(aws secretsmanager describe-secret \
  --secret-id youtube-lakehouse-redshift-credentials \
  --query ARN --output text)

aws redshift-data execute-statement \
  --workgroup-name youtube-lakehouse-wg \
  --database youtube_lakehouse \
  --sql "SELECT region, COUNT(*) FROM gold.category_daily_summary GROUP BY region" \
  --secret-arn "$SECRET_ARN"
```
Then `aws redshift-data describe-statement --id <Id>` and `get-statement-result` to see the counts.

Athena (per-video Silver detail — the Terraform already saved two queries for you):
```bash
aws athena start-query-execution \
  --query-execution-context Database=youtube_lakehouse \
  --work-group youtube-lakehouse-detail \
  --named-query-id "$(aws athena list-named-queries --work-group youtube-lakehouse-detail --query 'NamedQueryIds[0]' --output text)"
```
Or just open the Athena console, pick the `youtube-lakehouse-detail` workgroup, and run the saved `youtube-lakehouse-video-detail` or `youtube-lakehouse-likes-vs-comments` query directly — this is the easier path as a beginner.

### 9. dbt tests against the warehouse

```bash
cd dbt_project
python3 -m pip install -r ../requirements-dbt.txt
export DBT_PROFILES_DIR=$(pwd)
export REDSHIFT_HOST="$(cd ../terraform && terraform output -raw redshift_workgroup_endpoint)"
export REDSHIFT_USER='lakehouse_admin'
export REDSHIFT_PASSWORD="$TF_VAR_redshift_admin_password"
dbt deps
dbt seed --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### 10. Build the QuickSight dashboard

Terraform provisions the VPC connection, Redshift data source, and dataset — you build the visuals by hand (QuickSight analyses aren't cleanly Terraform-managed). In the QuickSight console:

1. Open the `Category Daily Performance` dataset (`youtube-lakehouse-category-daily-performance`) → **Create analysis**.
2. Build at minimum: a bar chart of `total_views` by `category_name`, a line chart of `video_count` by `trending_date` colored by `region`, and a pivot table of `total_views` / `avg_engagement_ratio` grouped by `region` → `category_name`.
3. **On the engagement-ratio field specifically:** set its aggregation to **Average**, not the default **Sum** — see the [Production Problems](#-production-problems-i-hit-and-how-i-fixed-them) log below for why this matters and what it looks like when it's wrong.
4. **Share → Publish dashboard.**

### 11. Capture proof before teardown

QuickSight access disappears the moment you `terraform destroy`. Screenshot everything **before** tearing anything down — see [Screenshots](#-screenshots) below for exactly what to capture and where it goes.

## 🔄 Data Flow

### Bronze (raw landing)
Untransformed CSVs land at `s3://<bucket>/bronze/youtube/<REGION>videos.csv`. No schema enforcement here beyond the S3 event trigger.

### Silver (validated, cleaned, deduplicated) — `bronze_to_silver.py`
- Spark reads the Bronze CSV distributed (no `.collect()` onto the driver).
- Each row is validated **partition-by-partition** via `validate_and_clean_row`: required fields present, numeric fields parseable and non-negative, `trending_date` parseable, region resolved from filename.
- Rows that fail any check are quarantined to `s3://<bucket>/quarantine/youtube/` partitioned by `quarantine_reason` — nothing is silently dropped.
- Surviving rows are deduplicated on `(video_id, trending_date, region)` — this grain matters: it's what stops a legitimately repeated video on a different day, or the same video trending in two different regions, from being treated as an accidental duplicate.
- A **data-quality report** (`total_bronze_rows`, `validity_rate`, `duplicate_rate`, `reasons` breakdown) is written to S3 and read back by Step Functions to decide whether to continue.
- Category names are enriched via a live YouTube Data API v3 call, with the last-known-good reference JSON in S3 as a fallback if the API call fails.
- Output is written to `s3://<bucket>/silver/youtube/`, partitioned by `region`/`trending_date`, with **dynamic partition overwrite** — a fresh run for one region/date only replaces that partition, leaving every other region and date untouched.

### Gold (analytics-ready) — `silver_to_gold.py`
- Every run re-reads the **entire** Silver dataset (all regions, all history so far) and does a full distributed `groupBy(category_id, trending_date, region)` aggregation — `video_count`, `total_views`, `total_likes`, `total_dislikes`, `total_comments`, `avg_views_per_video`, `avg_engagement_ratio`.
- Category names are joined in **after** aggregation (joining before it would get silently dropped by the groupBy — it's neither a grouping key nor an aggregate).
- Unknown category IDs are labeled explicitly (`Unknown (24)`) rather than dropped.
- Written to S3 Gold (partitioned, overwrite) and loaded into Redshift via JDBC using a stage-table `TRUNCATE` + `INSERT` pattern — this is a **full refresh, not an incremental append**, which is what keeps every run consistent with zero risk of double-counting or partial state.

### Serving
- **Redshift Serverless** (private VPC, 3 AZs, `publicly_accessible = false`) holds `gold.category_daily_summary` as the primary analytics table.
- **Athena** queries Silver Parquet directly for ad hoc per-video detail without forcing everything through Redshift.
- **QuickSight**, VPC-connected to Redshift, serves the published dashboard.
- **dbt** runs generic + business-rule tests against the warehouse tables (uniqueness on the `(category_id, trending_date, region)` grain, non-null checks, accepted-value checks on `region`).

## 🔧 Production Problems I Hit — And How I Fixed Them

This section is the part I actually think is worth reading. Anyone can post a working pipeline; here's what actually went wrong building it and how each was diagnosed and closed, in the order I hit them — starting with the AWS account that never worked at all, before any of the code-level problems below.

### 1. The first AWS account was never viable — Redshift and Glue blocked at the account level
**Symptom:** `terraform apply` failed in three different ways on the same account, over roughly two weeks: `SubscriptionRequiredException` on `CreateNamespace` for Redshift Serverless, `AccessDeniedException: Account <id> is denied access` on every Glue job/crawler creation, and `ResourceNotFoundException: Account information for account <id> is not found` on QuickSight — despite the IAM user/role having full `AdministratorAccess`.
**Diagnosis:** Three unrelated-looking errors, one root cause each, none of them an IAM problem:
- Since AWS split new accounts into Free/Paid Plan (July 2025), **Redshift is excluded from the Free Plan outright** — not rate-limited, not a trial restriction, just unavailable until the account is upgraded to Paid.
- Glue's `AccessDeniedException` is a **backend fraud-prevention hold** AWS places on new/recently-upgraded accounts for certain compute-provisioning APIs (crawler/job creation specifically), to block abuse like crypto-mining. It happens before IAM is ever evaluated, so no policy change fixes it — the exact same Terraform that failed one day applied cleanly with zero code changes once AWS lifted the hold.
- QuickSight's error was simpler: the account signup itself had failed with a generic "Oops" error and never actually completed.
**Fix:** Upgraded the account to the Paid Plan (confirmed this doesn't forfeit existing free-tier credits or "Always Free" allowances — it's a status change, not a purchase). For the Glue hold specifically, filed an AWS Support case under Billing & Accounts and waited for AWS to manually clear it — a real multi-day wait, not something retriable from the terminal. Even after the Paid Plan upgrade and the Glue hold clearing, the account accumulated enough tangled partial state (see #5 below) that it was eventually more practical to build cleanly on a second AWS account than keep excavating the first one.
**Why this matters:** this is the actual reason this repo exists as an "AWS-native rebuild" — the earlier local-only version of this project (Airflow/Spark/Postgres/Streamlit) worked around exactly this by never touching Glue or Redshift Serverless at all.

### 2. AWS security-group rule rejected — invalid characters
**Symptom:** `terraform apply` failed adding an ingress rule description.
**Cause:** AWS security-group description fields only accept a limited character set — an em dash (`—`) and an apostrophe in my description text weren't allowed.
**Fix:** Rewrote the description in plain ASCII. Small, but a good reminder that Terraform errors are usually AWS API-level constraints, not Terraform bugs — read the actual error text before assuming the tool is wrong.

### 3. Wrong security group edited entirely
**Symptom:** Same error category as above, but fixing the syntax didn't fix the underlying problem.
**Cause:** I'd pasted the ingress rule into `aws_security_group.glue_endpoints` (VPC interface endpoint HTTPS access) instead of the Redshift-access security group. Wrong resource, not just wrong syntax.
**Fix:** Confirmed via `terraform plan` diff which resource was actually being changed before applying, and moved the rule to the correct security group in `networking.tf`.

### 4. Stray Terraform syntax error from a bad copy-paste
**Symptom:** `Error: Missing newline after block definition`.
**Cause:** A closing `}` from one resource block got merged onto the same line as the next resource's first attribute during a manual edit.
**Fix:** Rewrote the affected file cleanly rather than patching around it — for HCL, when a paste goes wrong it's faster to replace the whole file than to hunt for the exact character.

### 5. A wiped local Terraform state cascaded into ~10 separate "already exists" errors
**Symptom:** After clearing local Terraform state to work around an earlier networking tangle, every subsequent `terraform apply` failed on a *different* resource each time: `EntityAlreadyExists` (IAM roles), `ResourceExistsException` (Secrets Manager), `AlreadyExistsException` (Glue Catalog database), `ConflictException` (Redshift namespace, then workgroup), `ResourceAlreadyExistsException` (CloudWatch log group), `InvalidRequestException: WorkGroup is already created` (Athena), `ResourceConflictException` (Lambda function, then separately its S3-invoke permission) — plus, unrelated but from the same repeated-failed-build cycle, `VpcLimitExceeded` (old half-built VPCs from earlier attempts eating the 5-per-region cap) and `InvalidSubnet.Conflict` on overlapping CIDR blocks.
**Cause:** Deleting local Terraform state doesn't delete the real AWS resources it was tracking — it just makes Terraform forget it owns them. The next `plan` tries to create everything from scratch and collides with what's already live, one resource at a time as each successive error is fixed.
**Fix:** Built a dedicated `imports.tf` and used Terraform `import` blocks to reattach each already-existing resource to its Terraform address — VPC, Redshift namespace and workgroup, IAM roles, Secrets Manager secrets, CloudWatch log group, Athena workgroup, Glue Catalog database, Lambda function — instead of trying to delete and recreate real infrastructure. Manually deleted the genuinely orphaned dead VPCs in the console first to clear the VPC-limit block.
**Takeaway:** never wipe local Terraform state to "start clean" once real resources exist behind it — `terraform import` is the correct tool. This one shortcut is what turned a single networking mistake into roughly a dozen cascading, unrelated-looking errors across almost every service in the stack.

### 6. Step Functions IAM role not authorized to access the Log Destination
**Symptom:** `AccessDeniedException: The state machine IAM Role is not authorized to access the Log Destination` on `CreateStateMachine`, persisting across several `apply` attempts.
**Diagnosis:** A hand-added `aws_cloudwatch_log_resource_policy` block was using a dynamic ARN with a trailing wildcard (`${aws_cloudwatch_log_group.sfn_logs.arn}:*`) directly inside the policy JSON — CloudWatch Log Resource Policies reject variable/wildcard configurations in that position, so the policy silently failed to register, which meant Step Functions still had no grant to write to the log group.
**Fix:** Removed the ad-hoc resource-policy block entirely and instead scoped the project's existing `data "aws_iam_policy_document" "sfn_log_delivery"` block's `resources` list explicitly to the log group's ARN (both the bare ARN and the `:*` variant, as separate list entries rather than one wildcarded string) — using the built-in policy-document pattern the project already had, instead of a hand-rolled one.

### 7. Redshift connection test timing out from a BI tool
**Symptom:** `Database Error: ('connection time out', TimeoutError(110, 'Connection timed out'))`.
**Cause:** Networking path into the private Redshift Serverless workgroup wasn't fully wired for the client trying to reach it.
**Fix:** Verified and corrected the security group / subnet routing so the connection resolved in seconds instead of hanging for ~5 minutes before failing.

### 8. QuickSight Terraform schema drift against the pinned provider version
**Symptom:** `Error: Insufficient parameters blocks` / `Error: Unsupported block type` on the QuickSight data-source and data-set resources.
**Cause:** The QuickSight resource schema differs between provider versions, and my HCL didn't match the block names/nesting for the `hashicorp/aws` version actually pinned in `.terraform.lock.hcl` (`permissions(data_set)` vs `permission(data_source)`, `parameters` vs `data_source_parameters`).
**Fix:** Checked the exact provider docs for the pinned version (6.62.0) rather than the latest docs online, and corrected the block structure to match.

### 9. QuickSight account registration stuck on a generic error
**Symptom:** QuickSight signup failed with a non-specific "Oops" error, more than once, blocking every `aws_quicksight_*` resource with `ResourceNotFoundException`.
**Cause:** Turned out to be an AWS-side platform issue tied to the account/name combination, not a retriable client error.
**Fix:** Used a fresh, collision-proof account name (previously-tried names can stay "reserved" for a while even after a failed signup) and it went through cleanly on retry. Lesson: after a couple of identical failures with no new information, it's a support-case situation, not a "try again" situation.

### 10. QuickSight `CreateDataSource` — unsupported resource-permissions state
**Symptom:** `InvalidParameterValueException: Resultant state of ResourcePermissions on this resource is not supported.`
**Cause:** The `actions` list on the QuickSight permissions block was missing `DeleteDataSource`, so it matched neither of QuickSight's two accepted permission sets exactly.
**Fix:** Matched the actions list exactly to one of AWS's supported permission sets.

### 11. QuickSight data source stuck in `CREATION_FAILED` — `GENERIC_SQL_FAILURE`
**Symptom:** `unexpected state 'CREATION_FAILED' ... GENERIC_SQL_FAILURE: The connection attempt failed.`
**Cause:** The Redshift Serverless workgroup had `publicly_accessible = true`. Counter-intuitively, that breaks in-VPC clients like a QuickSight VPC-connection ENI, because the private-DNS resolution path AWS sets up for VPC-internal clients isn't configured the same way when the workgroup is also publicly routable.
**Fix:** Flipped the workgroup to `publicly_accessible = false` (AWS's own recommended pattern for QuickSight-via-VPC-connection). Confirmed by the data source moving to `Creation complete` on the next apply.

### 12. QuickSight `CreateDataSet` — physical table map key regex
**Symptom:** `ValidationException: ... Map keys must satisfy constraint: ... pattern: [0-9a-zA-Z-]*`
**Cause:** My `physical_table_map` key used an underscore, which QuickSight's key regex doesn't allow (only alphanumerics and hyphens).
**Fix:** Renamed the key to use a hyphen instead of an underscore.

### 13. QuickSight `CreateDataSet` — resource-permissions mismatch again, different resource
**Symptom:** Same `ResourcePermissions` error as #10, this time on the data set instead of the data source.
**Cause:** Same root cause as #10 — the actions list didn't exactly match one of QuickSight's two accepted permission sets.
**Fix:** Same fix, applied to the data-set resource's permission block.

### 14. The big one: pipeline failed loading `INvideos.csv` — data quality gate
**Symptom:** Step Functions execution ended in `PipelineFailed`, cause: *"YouTube Lakehouse batch pipeline failed or was blocked by the data quality gate."* US and GB had already loaded successfully; IN was the first file to actually trip the gate.
**Diagnosis:** Pulled the DQ report the job had already written to S3 (`dq-reports/bronze-to-silver/<run-id>.json`) instead of guessing from the error message alone:
```json
{
  "total_bronze_rows": 37352,
  "validated_clean_rows": 32458,
  "duplicate_rows": 4894,
  "pass_rate": 0.868976,
  "threshold": 0.95,
  "pass": false,
  "reasons": { "DUPLICATE_ROW": 4894 },
  "rejected_validation_rows": 0
}
```
`rejected_validation_rows` was **zero** — every row in the file was structurally valid. The entire shortfall was 4,894 exact duplicate rows (13.1% of the file), a known characteristic of the India export in this Kaggle dataset. The original DQ formula scored duplicate rows the same as genuinely corrupt data, so a file that was 100% structurally valid still failed the gate purely because it had a naturally higher duplicate rate than US/GB.
**Fix:** Changed `data_quality_report()` in `transform_logic.py` to compute `pass` from a `validity_rate` — `(clean_rows + duplicate_rows) / total_rows` — instead of `clean_rows / total_rows`. Duplicates are still fully quarantined out of Silver (dedup behavior didn't change); they're just no longer scored as if they were data corruption when deciding whether the file is trustworthy. `duplicate_rate` is still reported separately for visibility. All 61 existing unit tests still passed unmodified, and re-running the numbers above through the new formula gives `validity_rate = 1.0` — correctly reflecting that the file was clean. Re-uploaded the same `INvideos.csv`; it went straight through to `PipelineSucceeded`.
**Why I'm keeping this in the README instead of hiding it:** a DQ gate that never once triggers isn't proof the gate works — it's proof it was never tested against real messy data. This is the strongest evidence in the whole project that the design does what it's supposed to do.

### 15. Dashboard showing an engagement ratio over 100 (physically impossible)
**Symptom:** The QuickSight pivot table showed `avg_engagement_ratio` = 105.29 for the US region — `(likes + comments) / views` should realistically sit well under 1 for the overwhelming majority of videos.
**Diagnosis:** Checked the actual computation in both `transform_logic.py` and the Spark aggregation in `silver_to_gold.py` — `avg_engagement_ratio` is correctly computed as an average per `(category, date, region)` bucket, each one a small decimal. The bug wasn't in the pipeline at all: the pivot table's column header literally read *"Sum of Avg_engagement_ratio"*. QuickSight's default field aggregation is `SUM`, and the table was rolled up across every category **and every one of ~200 trending dates** — summing ~200+ small daily averages is exactly how you get to 100+.
**Fix:** Changed the field's aggregation in the QuickSight visual from `Sum` to `Average`. No pipeline or Terraform change needed. (For a more statistically rigorous version, a calculated field of `sum(total_likes + total_comments) / sum(total_views)` avoids the average-of-averages issue entirely — noted here as a possible future refinement, not required for correctness at this scale.)

## ✅ Key Takeaways

- Demonstrates the Medallion Architecture (Bronze → Silver → Gold) on **real managed AWS services**, not local emulation.
- Step Functions owns orchestration, retries, DQ branching, and failure notification — Lambda stays intentionally thin (a single job: wake up the state machine).
- Distributed Spark throughout — validation via `mapPartitions`, aggregation via native `groupBy`; the full dataset is never collected onto a driver.
- An explicit, tested, and **actually-triggered** data-quality gate with S3-quarantine, not just a checkbox that's always green.
- Redshift Serverless runs fully private (no public endpoint) with a VPC-connected QuickSight dashboard on top.
- dbt tests enforce grain uniqueness and business rules directly against the warehouse.
- Full CI (pytest + `terraform fmt`/`validate` + `dbt parse`) on every push.
- Every credential (API key, DB password) lives in Secrets Manager or an environment variable — never in a tracked file.

## ⚖️ Trade-offs & Design Decisions

**Region resolved from filename, not content.** Simple and explicit, at the cost of requiring the uploader to keep Kaggle's original naming convention. The alternative (sniffing region from file content) would be more forgiving but far more fragile and harder to reason about.

**Full-refresh Gold recompute, not incremental.** Every Silver→Gold run reprocesses the entire historical Silver dataset rather than just the newly arrived partition. At this data volume the cost is trivial, and it completely eliminates a category of incremental-pipeline bugs (partial state, double-counting, drift between runs). At YouTube-actual scale this would need to become incremental with careful watermarking — a deliberate, stated trade-off for a portfolio-scale project.

**Duplicate rows quarantined, not scored as corruption.** Chose to treat "the source file has redundant rows" and "the source file has malformed rows" as two different signals (see Production Problem #11) rather than lowering the DQ threshold to make the symptom go away. A lower threshold would have hidden genuinely bad data too; a formula that distinguishes the two doesn't.

**Redshift Serverless over provisioned Redshift.** No cluster to size or pause/resume manually, and it scales to (near) zero cost between demo runs — the right trade for a portfolio project that isn't run continuously. A production workload with predictable, heavy concurrent load might do better on provisioned RA3 nodes with reserved pricing.

**QuickSight over a code-first dashboard.** Chose QuickSight specifically to demonstrate AWS-native BI and VPC-connected access to a private warehouse, versus a Streamlit/Altair dashboard (which is what the earlier local version used). Trade-off: QuickSight analyses aren't cleanly Terraform-managed, so the visual build step is manual and undocumented-as-code — noted explicitly rather than glossed over.

**Terraform for everything except the QuickSight analysis/dashboard itself.** Data sources, datasets, and the VPC connection are all Terraform-managed; the analysis/visual layer is a one-time manual build, because Terraform's QuickSight analysis support is thin and account-specific (template ARNs, etc.) in a way that doesn't reproduce cleanly across accounts.

## 🔒 Sensitive Info & How I Push This Safely

This project touches real AWS account IDs, a Redshift admin password, a YouTube API key, and Terraform state that contains resource ARNs. None of that belongs in git history. Here's exactly how it's kept out.

### What's already gitignored (never staged)
```
.venv/  venv/  __pycache__/  .pytest_cache/  *.pyc  *.pyo
.terraform/
terraform.tfstate
terraform.tfstate.*
terraform.tfvars
*.tfvars.json
*.auto.tfvars
.env
terraform/*.zip
dbt_project/target/
dbt_project/dbt_packages/
dbt_project/logs/
sample_data/*.csv
sample_data/*.json
```
`terraform.tfstate` in particular can contain secret values in plaintext depending on the resource — it must **never** be committed. Only `terraform.tfvars.example` (placeholders only, no real values) is tracked.

### Before every `git push`, run this checklist
```bash
git status
```
Confirm **none** of these show up as staged or untracked-but-about-to-be-added: `.venv/`, `.terraform/`, `terraform.tfstate*`, `terraform.tfvars`, `dbt_project/target/`, `dbt_project/dbt_packages/`, any `*.zip` under `terraform/`.

```bash
git diff --cached | grep -iE "AKIA[0-9A-Z]{16}|aws_secret_access_key|secret_string|password\s*=\s*['\"]"
```
This should return nothing. If it returns something, **unstage it and fix the source** before committing — don't commit-then-fix, since the secret is then in history even if you remove it in a later commit.

### Where secrets actually live instead
- **Redshift admin password** — never written to disk. Supplied only via `TF_VAR_redshift_admin_password` in the shell environment for the duration of `terraform apply`.
- **YouTube API key** — stored in AWS Secrets Manager (`youtube-lakehouse-youtube-data-api-key`), read at runtime by the Glue job via `boto3`. Never appears in code, Terraform, or CI.
- **AWS credentials for CI** — GitHub Actions only runs `pytest` + `terraform fmt`/`validate` + `dbt parse`, none of which require live AWS credentials, so no AWS secret is stored in GitHub at all.

### If a secret ever does slip into a commit
Don't just delete it in a new commit — the old commit still has it in history. Rotate the credential immediately (new API key / new Redshift password), then use `git filter-repo` or BFG Repo-Cleaner to purge it from history before the next push, and force-push only after confirming no one else has pulled the bad history.

### Account IDs / ARNs in this README and docs
The AWS account ID and ARNs shown in this README's command examples are illustrative placeholders — replace them with your own account's values when you run these commands. They aren't secrets in the same sense as a password or API key, but there's no reason to publish a real account ID either, so they've been genericized here.

## 📸 Screenshots

This is the evidence a reviewer actually looks for — a README full of claims is worth far less than proof each piece really ran. Capture these **before** teardown, save into `screenshots/` with the names below, then they render inline in this file.

**Pipeline evidence (do these first — they prove the orchestration actually works):**

```
screenshots/
├── stepfunctions-graph-succeeded.png       # graph view of a full green run — the single most important shot
├── stepfunctions-execution-history.png     # shows the DataQualityGate branch decision
├── stepfunctions-pipeline-failed-dq-gate.png   # optional — the IN failure, kept as proof of Production Problem #11
├── s3-bronze-silver-gold-quarantine.png    # bucket showing all four prefixes populated
├── glue-job-run-success.png                # a Bronze->Silver or Silver->Gold successful run's metrics
├── glue-crawler-and-catalog-table.png      # crawler result + resulting Silver table schema
├── redshift-query-editor-gold-counts.png   # Query Editor v2, SELECT on gold.category_daily_summary
├── athena-video-detail-query-result.png    # the saved video-detail query, results shown
├── secrets-manager-secret-names.png        # secret names only, never values
├── eventbridge-scheduled-rule.png          # the daily backstop rule
├── sns-subscription-confirmed.png          # confirmed email subscription
├── github-actions-ci-green.png             # a passing CI run
├── dashboard-bar-views-by-category.png
├── dashboard-line-trend-by-region.png
└── dashboard-pivot-region-category.png
```

### Pipeline orchestration

![Step Functions successful run](screenshots/stepfunctions-graph-succeeded.png)

*The DQ gate branch, and the actual failure it caught on the India file (Production Problem #11 above):*

![Step Functions execution history](screenshots/stepfunctions-execution-history.png)
![Step Functions DQ gate failure](screenshots/stepfunctions-pipeline-failed-dq-gate.png)

### Dashboard

| Views by category | Trend over time by region |
|---|---|
| ![Views by category](screenshots/dashboard-bar-views-by-category.png) | ![Trend by region](screenshots/dashboard-line-trend-by-region.png) |

**Regional breakdown (category × region, with `avg_engagement_ratio` correctly set to Average aggregation):**

![Regional breakdown](screenshots/dashboard-pivot-region-category.png)

## 🧪 Local Tests

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest tests/ -v
```
61 tests covering transformation logic, the DQ report formula (including the duplicate-vs-invalid distinction from Production Problem #11), region resolution, deduplication, API retry behavior, category-response parsing, and Lambda trigger behavior — all without an AWS account.

## 🧹 Teardown & Cost Control

QuickSight access disappears the moment the stack is destroyed, so capture screenshots first (see above). The S3 bucket is deliberately not `force_destroy`d, so empty it explicitly:

```bash
aws s3 rm "s3://$(cd terraform && terraform output -raw lakehouse_bucket_name)" --recursive
cd terraform
terraform destroy -var="redshift_admin_password=$TF_VAR_redshift_admin_password"
```
Read the destroy plan before confirming — same discipline as every apply. QuickSight's account subscription itself is separate from Terraform and isn't touched by `destroy`; cancel it separately in the QuickSight console if you're done with it entirely.

## 🗣️ Interview Story

- **Bronze:** preserve source data as-is; region is derived from filename, not inferred from content.
- **Silver:** validate, normalize, deduplicate, quarantine bad rows with reasons, write partitioned Parquet.
- **Gold:** distributed category/day/region aggregation, full-refresh into a private warehouse.
- **Orchestration:** Step Functions owns retries, DQ branching, crawler sequencing, and failure alerting — Lambda is intentionally thin.
- **Data quality:** a real gate that has actually triggered on real data (see Production Problem #11) and was refined based on what tripped it — not a pipeline that's simply never seen messy input.
- **Warehouse:** Redshift Serverless, fully private, VPC-connected BI.
- **Ad hoc:** Athena reads Silver detail without forcing everything through Redshift.
- **Testing:** Python unit tests cover functional logic; dbt covers warehouse constraints and business rules; CI runs both plus Terraform validation on every push.
- **Security:** Secrets Manager, private Redshift, encrypted S3, blocked public access, TLS-only S3, least-privilege runtime roles, and nothing sensitive ever committed to git.
