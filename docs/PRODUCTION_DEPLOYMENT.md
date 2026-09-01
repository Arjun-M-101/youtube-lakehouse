# Production-Grade Deployment Guide — YouTube Lakehouse Batch

This guide is the direct replacement for the older v3 workaround path. It assumes the AWS account now allows Glue resources, so Glue remains enabled throughout.

## 1. Architecture

```text
Kaggle CSV (USvideos.csv, GBvideos.csv, ...)
        |
        v
S3 Bronze
        |
 ObjectCreated
        v
     Lambda -------------- EventBridge daily backstop
        |                             |
        +-------------+---------------+
                      v
               Step Functions
                      |
          +-----------+-----------+
          |                       |
          v                       v
 Glue Bronze->Silver          DQ report
          |
          v
 S3 Silver / Parquet
          |
          +--> Glue Crawler --> Glue Data Catalog --> Athena detail
          |
          v
 Glue Silver->Gold
          |
     +----+-----+
     |          |
     v          v
S3 Gold    Redshift Serverless
                |
                +--> dbt tests/models
                +--> QuickSight
```

## 2. Prerequisites

- AWS account with access to create Glue, Redshift Serverless, Lambda, Step Functions, EventBridge Scheduler, Athena, Secrets Manager, and QuickSight resources.
- AWS CLI v2.
- Python 3.11+ locally.
- Terraform 1.7+.
- Kaggle account for the YouTube Trending Video Statistics dataset.
- YouTube Data API v3 key.
- QuickSight signup completed once.

The Terraform project pins the AWS provider to the current 6.x generation and uses Glue 5.1 for the ETL runtime.

## 3. AWS account and billing protection

Create a zero-spend budget/alert before deploying. It is an alert, not a hard spending limit. Always tear down the demo resources after verification.

## 4. AWS CLI authentication

For the personal sandbox flow, use the dedicated deployment identity described in the original v3 guide. Do not create root access keys.

```bash
aws configure
aws sts get-caller-identity
```

## 5. QuickSight one-time setup

QuickSight requires a one-time account signup before Terraform can create the VPC connection/data source/dataset resources.

```bash
aws quicksight list-users \
  --aws-account-id "$(aws sts get-caller-identity --query Account --output text)" \
  --namespace default
```

Copy the `Arn` value for your QuickSight user into `terraform/terraform.tfvars` as `quicksight_user_arn`.

## 6. Prepare Terraform

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

Set:

```hcl
aws_region                = "ap-south-1"
environment               = "dev"
project_name              = "youtube-lakehouse"
alert_email               = "your-real-email@example.com"
quicksight_user_arn       = "arn:aws:quicksight:ap-south-1:<account-id>:user/default/<user-name>"
min_dq_pass_rate          = 0.95
```

Set the password only in the shell:

```bash
export TF_VAR_redshift_admin_password='your-strong-password'
```

## 7. Deploy

```bash
terraform init
terraform fmt -recursive
terraform validate
terraform plan
terraform apply
```

After apply, confirm the SNS email subscription. Until the email subscription is confirmed, the alert path is not useful.

## 8. Store the YouTube API key

```bash
aws secretsmanager put-secret-value \
  --secret-id youtube-lakehouse-youtube-data-api-key \
  --secret-string 'YOUR_YOUTUBE_API_KEY'
```

The API key is not hardcoded in Python or Terraform source.

## 9. Local unit tests

```bash
cd ..
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest tests/ -v
```

The unit test suite is intentionally independent of live Glue/Spark/AWS APIs. It covers the functional core.

## 10. Upload source data

Use the original country-specific filenames.

```bash
BUCKET=$(cd terraform && terraform output -raw lakehouse_bucket_name)
aws s3 cp sample_data/USvideos.csv "s3://${BUCKET}/bronze/youtube/USvideos.csv"
aws s3 cp sample_data/GBvideos.csv "s3://${BUCKET}/bronze/youtube/GBvideos.csv"
```

One country proves the full path. Two or more countries prove that region is a true part of the analytical grain.

Do not rename the files to `trending.csv`.

## 11. Verify the orchestration

AWS Console -> Step Functions -> `youtube-lakehouse-batch-pipeline`.

Expected flow:

`BronzeToSilver -> ReadDataQualityReport -> DataQualityGate -> StartSilverCrawler -> WaitForCrawler -> GetCrawler -> SilverToGold -> PipelineSucceeded`

A DQ failure branches to an SNS notification and `PipelineFailed`.

## 12. Verify Glue

Check the Glue jobs:

- `youtube-lakehouse-bronze-to-silver`
- `youtube-lakehouse-silver-to-gold`

The Bronze job should write partitioned Parquet under:

`s3://<bucket>/silver/youtube/region=<REGION>/trending_date=<DATE>/`

Bad rows and duplicate rows go to the quarantine prefix with explicit `quarantine_reason` values.

## 13. Verify the DQ report

The Step Functions state machine reads the JSON report from:

`s3://<bucket>/dq-reports/bronze-to-silver/<execution-name>.json`

Important fields include:

- `total_bronze_rows`
- `validated_clean_rows`
- `rejected_validation_rows`
- `duplicate_rows`
- `pass_rate`
- `threshold`
- `pass`
- `reasons`

The default threshold is 0.95 and can be changed with `min_dq_pass_rate`.

## 14. Verify Redshift

```bash
aws redshift-serverless get-workgroup \
  --workgroup-name "$(cd terraform && terraform output -raw redshift_workgroup_name)"
```

Use Query Editor v2 and run:

```sql
SELECT *
FROM gold.category_daily_summary
ORDER BY total_views DESC
LIMIT 10;
```

For multi-country data:

```sql
SELECT DISTINCT region
FROM gold.category_daily_summary
ORDER BY region;
```

## 15. dbt validation

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

The dbt layer checks nullability, category referential integrity, non-negative metrics, uniqueness at `(category_id, trending_date, region)`, and two business rules: likes must not exceed views, and dislikes must not exceed views.

## 16. Athena detail path

After the first successful crawler run:

```bash
aws glue get-tables --database-name youtube_lakehouse
```

The crawler is expected to create a Silver table named `youtube`. Run the saved query from Athena workgroup `youtube-lakehouse-detail`.

The detail path intentionally reads Silver. Redshift remains the tested business-ready aggregate layer.

## 17. QuickSight

The Terraform stack creates:

- QuickSight VPC connection
- Redshift data source
- `Category Daily Performance` dataset

In QuickSight, create an analysis using the dataset. A useful first page is:

- bar: `category_name` vs `total_views`
- line: `trending_date` vs `total_views`
- filter: `region`

Publish the analysis as a dashboard.

## 18. Production hardening that this portfolio version demonstrates

### Data quality

Validation happens before Gold. Required fields are gated, numeric/date parsing is explicit, negative metrics are rejected, unknown regions are quarantined, and duplicates are retained as diagnostic quarantine records.

### Security

- S3 encryption at rest.
- S3 public access blocked.
- S3 bucket policy denies insecure transport.
- Redshift Serverless is private.
- Glue runtime credentials come from Secrets Manager.
- Glue runtime role does not have AdministratorAccess.
- Service roles are separated by function.

### Reliability

- Step Functions owns orchestration/retries/branching.
- Glue jobs have bounded retries and execution concurrency.
- Event-driven S3 trigger is supplemented by a daily backstop.
- SNS alerts are sent on DQ or orchestration failure.

### Scalability

- Bronze validation is partition-parallel.
- Gold aggregation is native Spark.
- Parquet is partitioned by region and date.
- The driver does not collect the full dataset.

### Testing

- Python unit tests for functional logic.
- API behavior tests with a mocked transport boundary.
- dbt warehouse-level tests for the real Redshift output.
- CI runs the test/validation suites on pull requests.

## 19. Terraform state

For a solo portfolio demo, local state keeps the deployment easy. For a shared production environment, use an encrypted S3 backend with locking and restricted state-bucket access. Terraform state can contain sensitive values, so it must be protected like credentials.

## 20. Teardown

Before destroy:

```bash
BUCKET=$(cd terraform && terraform output -raw lakehouse_bucket_name)
aws s3 rm "s3://${BUCKET}" --recursive
cd terraform
terraform destroy
```

Confirm no Redshift Serverless workgroup remains:

```bash
aws redshift-serverless list-workgroups --query 'workgroups[*].workgroupName'
```

Also check QuickSight separately; its account subscription is not equivalent to a Terraform resource destroy.
