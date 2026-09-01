# YouTube Lakehouse — AWS Batch Data Engineering Project

This is the rebuilt AWS-native version of the original YouTube Lakehouse batch project. It preserves the architecture and interview story from the v3 project while removing the workaround that disabled Glue in the old account.

## What this project demonstrates

- Amazon S3 data lake with Bronze / Silver / Gold medallion layers
- AWS Lambda as a thin S3 event trigger
- AWS Step Functions as the central batch orchestrator
- AWS Glue ETL (Glue 5.1 / Spark 3.5.6 / Python 3.11) for distributed transformations
- AWS Glue Data Catalog + crawler for Silver discovery
- Explicit data-quality validation and quarantine
- Configurable DQ gate (default pass threshold 95%)
- Amazon Redshift Serverless as the Gold warehouse
- Glue JDBC integration to a private Redshift Serverless workgroup
- Amazon Athena for per-video Silver detail queries
- Amazon QuickSight VPC-connected dashboard dataset
- dbt models, generic tests, and business-rule tests
- YouTube Data API v3 integration with retry/backoff and Secrets Manager
- CloudWatch logs + Step Functions failure alerts via SNS
- EventBridge Scheduler daily backstop
- GitHub Actions CI: pytest + Terraform fmt/validate + dbt parse

## End-to-end flow

S3 Bronze upload -> Lambda -> Step Functions -> Glue Bronze->Silver -> DQ report -> DQ Choice -> Glue crawler -> Glue Silver->Gold -> Redshift Serverless -> dbt validation / QuickSight.

A daily EventBridge Scheduler execution starts the same state machine with an empty `triggeredKey`, causing Bronze->Silver to process the full Bronze prefix as a backstop.

## Important design decisions

### Do not rename Kaggle files

Keep filenames such as `USvideos.csv`, `GBvideos.csv`, `INvideos.csv`. Region is resolved from the filename and becomes a real Silver/Gold column. A generic name such as `trending.csv` intentionally fails the `UNKNOWN_REGION` rule.

### Glue is real infrastructure, not a Lambda replacement

The previous working repo had Glue resources commented out and used Lambda-based data-processing workarounds. This release restores Glue ETL as the data-processing engine and keeps Lambda only as the thin event trigger.

### Distributed Spark, not driver-side `.collect()`

Bronze validation uses `mapPartitions`, and Gold aggregation is a native Spark `groupBy`. The full dataset is never collected into the Glue driver.

### Region is part of the grain

Silver dedupe uses `(video_id, trending_date, region)`. Gold uniqueness is `(category_id, trending_date, region)`. This prevents US and GB rows from being treated as accidental duplicates.

### Redshift is private

Redshift Serverless runs in a dedicated VPC with three Availability Zones, private subnets, security groups, and enhanced VPC routing. There is no public Redshift endpoint.

### QuickSight is included

Terraform creates the QuickSight VPC connection, Redshift data source, and dashboard-ready dataset. You still need the one-time QuickSight account signup and user ARN before `terraform apply`.

## Deployment order

1. Configure AWS CLI credentials.
2. Create the zero-spend billing alert.
3. Create/sign into QuickSight and obtain `quicksight_user_arn`.
4. Put `terraform.tfvars` values in place and export `TF_VAR_redshift_admin_password`.
5. Run `terraform init`, `terraform validate`, `terraform plan`, and `terraform apply`.
6. Confirm the SNS subscription email.
7. Put the YouTube API key into Secrets Manager.
8. Run the local unit tests.
9. Upload `USvideos.csv` (and optionally GB/CA/IN) to the Bronze prefix.
10. Verify Step Functions, Glue, the DQ report, Redshift Gold, Athena, and dbt.
11. Build/publish the QuickSight analysis.
12. Destroy the stack when the demo is finished.

## The only values you must supply

Copy `terraform/terraform.tfvars.example` to `terraform/terraform.tfvars` and replace the email/QuickSight ARN. Supply the Redshift password through an environment variable; never put it in the file.

```bash
export TF_VAR_redshift_admin_password='use-a-strong-password-here'
```

After Terraform creates the Secret Manager containers, store the YouTube API key:

```bash
aws secretsmanager put-secret-value \
  --secret-id youtube-lakehouse-youtube-data-api-key \
  --secret-string 'YOUR_YOUTUBE_API_KEY'
```

## Local tests

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest tests/ -v
```

The tests exercise transformation logic, DQ rules, region resolution, deduplication, API retry behavior, category-response parsing, and Lambda trigger behavior without requiring an AWS account.

## dbt

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

## Teardown

The S3 bucket is intentionally not `force_destroy`ed. Empty it before `terraform destroy`:

```bash
aws s3 rm "s3://$(cd terraform && terraform output -raw lakehouse_bucket_name)" --recursive
cd terraform
terraform destroy
```

If you keep using QuickSight between demonstrations, remember that its account subscription is separate from Terraform-managed data-source/dataset resources.

## Interview story

The project can be explained as a production-style batch lakehouse:

- **Bronze:** preserve source data and filename-derived region.
- **Silver:** validate, normalize, deduplicate, quarantine bad rows, and write Parquet partitions.
- **Gold:** distributed category/day/region aggregation for analytics.
- **Orchestration:** Step Functions owns retries, DQ branching, crawler sequencing, and alerts.
- **Warehouse:** Redshift Serverless provides a SQL analytics layer.
- **Ad hoc:** Athena reads Silver detail without forcing everything into Redshift.
- **BI:** QuickSight consumes the business-ready warehouse dataset.
- **Testing:** Python unit tests cover functional logic; dbt covers warehouse constraints and business rules.
- **Security:** Secrets Manager, private Redshift, encrypted S3, blocked public access, TLS-only S3, and least-privilege runtime roles.
