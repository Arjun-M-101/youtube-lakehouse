# Upgrade Notes — Rebuilt AWS Batch Project

This release is intentionally **not** a cosmetic v3 patch. It restores the intended AWS architecture and fixes the implementation gaps found in the uploaded current repository.

## Restored / retained

- AWS Glue Bronze->Silver ETL
- AWS Glue Silver->Gold ETL
- Glue Data Catalog + Silver crawler
- Step Functions orchestration
- S3 ObjectCreated -> Lambda trigger
- EventBridge daily backstop
- Redshift Serverless Gold warehouse
- Athena Silver detail path
- QuickSight integration
- dbt models/tests
- YouTube Data API integration
- SNS DQ/failure alerts
- Local unit tests + GitHub Actions CI

## Removed from the old repo

- Lambda-based Bronze->Silver/Silver->Gold data-processing workaround
- Commented-out Glue resources
- Glue role AdministratorAccess override
- Account-specific Terraform import blocks
- Terraform state and backup files
- Real `terraform.tfvars`
- Old generated Lambda ZIP/build output
- Old `.venv` / `.terraform` directories

## Engineering fixes

1. **Glue is distributed again.** The old code used full-data `.collect()` calls. The new Bronze job validates by Spark partition and the Gold job aggregates with Spark SQL functions.
2. **Multi-country correctness.** Region is part of the Silver dedupe key as well as the Gold uniqueness grain.
3. **Idempotent region/date refresh.** Spark dynamic partition overwrite preserves unaffected countries/dates while safely replacing partitions processed by the current run.
4. **DQ quarantine is explicit.** Validation errors and duplicate rows are both written to quarantine with a reason.
5. **All-invalid input is handled.** The job still emits a DQ report instead of failing while trying to infer an empty Spark schema.
6. **Private Redshift path.** Three private subnets, security groups, S3 gateway endpoint, and interface endpoints support the VPC-connected Glue JDBC path without a NAT Gateway.
7. **Secret separation.** YouTube API and Redshift credentials are stored in Secrets Manager rather than source code.
8. **QuickSight is core infrastructure.** The stack provisions the VPC connection, Redshift data source, and dataset instead of leaving them in an optional directory.
9. **Production observability.** Step Functions logs, Lambda log retention, job insights/observability options, and SNS failure notifications are included.

## Verification status

Local Python regression suite: **80 passed**.

Terraform execution against AWS was **not performed from this environment** because no AWS credentials were provided to the build environment. The Terraform configuration was reviewed statically and against the current AWS/HashiCorp resource schemas used for the design.

The first real-account verification remains `terraform init && terraform validate && terraform plan`, followed by `terraform apply` in the AWS account that you confirmed supports Glue.
