# YouTube Lakehouse — AWS Production-Grade Batch Delivery

## Included
- Amazon S3 lakehouse with Bronze/Silver/Gold, quarantine, DQ reports, reference and staging prefixes
- AWS Lambda as a thin S3-event trigger only
- AWS Step Functions orchestration with DQ branching, crawler wait/retry, success/failure handling and scheduled backstop
- AWS Glue 5.1 Spark ETL jobs for Bronze→Silver and Silver→Gold
- AWS Glue Data Catalog crawler
- Amazon Redshift Serverless warehouse with private 3-AZ networking and enhanced VPC routing
- Athena detail workgroup and saved queries over Silver
- QuickSight VPC connection, Redshift data source and Category Daily Performance dataset
- Secrets Manager for YouTube API and Redshift credentials
- SNS + CloudWatch logging/alerts
- dbt model validation and custom business-rule tests
- GitHub Actions CI checks
- Unit/API parsing tests and deployment documentation

## Deliberately excluded
- Terraform state files
- Real terraform.tfvars
- AWS credentials, API keys and passwords
- Account-specific Terraform import/state artifacts
- The previous Lambda ETL workaround
- Generated Python bytecode / test caches

## Verification performed in the build environment
- `pytest -q tests/` → 80 passed
- `python3 -m compileall -q src tests` → passed
- Step Functions ASL JSON syntax → passed
- Static hygiene scan → no old AWS account ID, no Terraform state, no real tfvars, no AdministratorAccess runtime role, two active Glue jobs, private Redshift, three AZ subnets, QuickSight resources present

## AWS-side limitation
This environment does not have access to the user's AWS account and no credentials are included. Therefore `terraform apply`, live Glue execution, Redshift queries, Athena execution and QuickSight publication must be performed in the user's AWS account using the supplied deployment guide.
