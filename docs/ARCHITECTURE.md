# Architecture Notes

## Why this is the right AWS version of the original project

The project keeps the same learning narrative as the open-source batch version but maps every major stage to AWS-managed services.

| Concern | AWS implementation |
|---|---|
| Raw storage | S3 Bronze |
| Validation / normalization | Glue Spark |
| Rejected rows | S3 Quarantine |
| Curated lake data | S3 Silver Parquet |
| Catalog | Glue Data Catalog |
| Orchestration | Step Functions |
| Event trigger | Lambda |
| Backstop trigger | EventBridge Scheduler |
| Warehouse | Redshift Serverless |
| Ad-hoc detail | Athena |
| Semantic SQL layer | dbt on Redshift |
| BI | QuickSight |
| Alerts | SNS |
| Secrets | Secrets Manager |
| Logs | CloudWatch Logs |

## Data grain

### Silver

One row per `(video_id, trending_date, region)` after validation/deduplication.

### Gold

One row per `(category_id, trending_date, region)`.

This is important because a category can trend on the same date in multiple countries and should not collide in a uniqueness test.

## Runtime boundaries

The S3-trigger Lambda is intentionally small. It does not transform data or call Glue itself; it starts one Step Functions execution. Step Functions owns the workflow contract, retries, DQ gate, crawler sequencing and alerts.

The Bronze->Silver Glue job is not placed in the private VPC because it intentionally calls the YouTube Data API. The Silver->Gold Glue job uses the private Redshift Glue connection and therefore runs through the project VPC.

## Failure behavior

- Invalid rows -> quarantine + DQ report.
- Duplicate rows -> quarantine with `DUPLICATE_ROW` + DQ report.
- DQ pass rate below threshold -> SNS alert + pipeline failure.
- Glue/crawler/runtime error -> Step Functions retry, then SNS alert + pipeline failure.
- YouTube API failure -> use the existing S3 category reference and continue, so a temporary external API outage does not destroy an otherwise valid batch.

## Production follow-up

For a team deployment, use a remote encrypted Terraform backend, IAM Identity Center/assume-role instead of a long-lived admin user, a separate read-only Redshift BI user, formal GitHub OIDC deployment, centralized log retention policies, and a formal approval gate before infrastructure apply.
