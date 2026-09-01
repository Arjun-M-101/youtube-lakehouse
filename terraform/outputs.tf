output "lakehouse_bucket_name" {
  value = aws_s3_bucket.lakehouse.bucket
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.batch_pipeline.arn
}

output "lambda_trigger_function_name" {
  value = aws_lambda_function.trigger_pipeline.function_name
}

output "glue_job_bronze_to_silver_name" {
  value = aws_glue_job.bronze_to_silver.name
}

output "glue_job_silver_to_gold_name" {
  value = aws_glue_job.silver_to_gold.name
}

output "silver_crawler_name" {
  value = aws_glue_crawler.silver_crawler.name
}

output "redshift_workgroup_name" {
  value = aws_redshiftserverless_workgroup.lakehouse.workgroup_name
}

output "redshift_database_name" {
  value = aws_redshiftserverless_namespace.lakehouse.db_name
}

output "redshift_workgroup_endpoint" {
  value = aws_redshiftserverless_workgroup.lakehouse.endpoint[0].address
}

output "athena_detail_workgroup_name" {
  value = aws_athena_workgroup.detail_queries.name
}

output "quicksight_dataset_id" {
  value = aws_quicksight_data_set.category_daily_performance.data_set_id
}

output "trigger_upload_command" {
  value = "aws s3 cp sample_data/USvideos.csv s3://${aws_s3_bucket.lakehouse.bucket}/bronze/youtube/USvideos.csv"
}

output "bronze_path" {
  value = "s3://${aws_s3_bucket.lakehouse.bucket}/bronze/youtube/"
}

output "silver_path" {
  value = "s3://${aws_s3_bucket.lakehouse.bucket}/silver/youtube/"
}

output "gold_path" {
  value = "s3://${aws_s3_bucket.lakehouse.bucket}/gold/category_daily_summary/"
}

output "quarantine_path" {
  value = "s3://${aws_s3_bucket.lakehouse.bucket}/quarantine/youtube/"
}
