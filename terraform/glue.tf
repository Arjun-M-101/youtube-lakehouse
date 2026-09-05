resource "aws_glue_catalog_database" "lakehouse" {
  name        = "youtube_lakehouse"
  description = "AWS Glue Data Catalog database for the YouTube medallion lakehouse"
}

resource "aws_s3_object" "transform_logic_module" {
  bucket       = aws_s3_bucket.lakehouse.id
  key          = "glue-scripts/transform_logic.py"
  source       = "${path.module}/../src/transform_logic.py"
  etag         = filemd5("${path.module}/../src/transform_logic.py")
  content_type = "text/x-python"
}

resource "aws_s3_object" "category_enrichment_module" {
  bucket       = aws_s3_bucket.lakehouse.id
  key          = "glue-scripts/category_enrichment.py"
  source       = "${path.module}/../src/category_enrichment.py"
  etag         = filemd5("${path.module}/../src/category_enrichment.py")
  content_type = "text/x-python"
}

resource "aws_s3_object" "api_client_module" {
  bucket       = aws_s3_bucket.lakehouse.id
  key          = "glue-scripts/api_client.py"
  source       = "${path.module}/../src/api_client.py"
  etag         = filemd5("${path.module}/../src/api_client.py")
  content_type = "text/x-python"
}

resource "aws_s3_object" "bronze_to_silver_script" {
  bucket       = aws_s3_bucket.lakehouse.id
  key          = "glue-scripts/bronze_to_silver.py"
  source       = "${path.module}/../src/glue_jobs/bronze_to_silver.py"
  etag         = filemd5("${path.module}/../src/glue_jobs/bronze_to_silver.py")
  content_type = "text/x-python"
}

resource "aws_s3_object" "category_reference_seed" {
  bucket       = aws_s3_bucket.lakehouse.id
  key          = "reference/categories/youtube_categories.json"
  source       = "${path.module}/../reference/youtube_categories.json"
  etag         = filemd5("${path.module}/../reference/youtube_categories.json")
  content_type = "application/json"
}

resource "aws_s3_object" "silver_to_gold_script" {
  bucket       = aws_s3_bucket.lakehouse.id
  key          = "glue-scripts/silver_to_gold.py"
  source       = "${path.module}/../src/glue_jobs/silver_to_gold.py"
  etag         = filemd5("${path.module}/../src/glue_jobs/silver_to_gold.py")
  content_type = "text/x-python"
}

resource "aws_glue_connection" "redshift" {
  name = "${var.project_name}-redshift"

  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:redshift://${aws_redshiftserverless_workgroup.lakehouse.endpoint[0].address}:5439/youtube_lakehouse"
    SECRET_ID           = aws_secretsmanager_secret.redshift_credentials.arn
    JDBC_ENFORCE_SSL    = "true"
  }

  physical_connection_requirements {
    availability_zone      = aws_subnet.lakehouse_a.availability_zone
    subnet_id              = aws_subnet.lakehouse_a.id
    security_group_id_list = [aws_security_group.glue_components.id]
  }

  depends_on = [
    aws_redshiftserverless_workgroup.lakehouse,
    aws_secretsmanager_secret_version.redshift_credentials,
  ]
}

resource "aws_glue_crawler" "silver_crawler" {
  name          = "${var.project_name}-silver-crawler"
  database_name = aws_glue_catalog_database.lakehouse.name
  role          = aws_iam_role.glue_job_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.lakehouse.bucket}/silver/youtube/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }
}

resource "aws_glue_job" "bronze_to_silver" {
  name         = "${var.project_name}-bronze-to-silver"
  role_arn     = aws_iam_role.glue_job_role.arn
  glue_version = "5.1"
  worker_type  = "G.1X"

  number_of_workers = var.glue_worker_count
  timeout           = 30
  max_retries       = 1

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lakehouse.bucket}/${aws_s3_object.bronze_to_silver_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language" = "python"
    "--extra-py-files" = join(",", [
      "s3://${aws_s3_bucket.lakehouse.bucket}/${aws_s3_object.transform_logic_module.key}",
      "s3://${aws_s3_bucket.lakehouse.bucket}/${aws_s3_object.category_enrichment_module.key}",
      "s3://${aws_s3_bucket.lakehouse.bucket}/${aws_s3_object.api_client_module.key}",
    ])
    "--BRONZE_PATH"                      = "s3://${aws_s3_bucket.lakehouse.bucket}/"
    "--SILVER_PATH"                      = "s3://${aws_s3_bucket.lakehouse.bucket}/silver/youtube/"
    "--QUARANTINE_PATH"                  = "s3://${aws_s3_bucket.lakehouse.bucket}/quarantine/youtube/"
    "--DQ_REPORT_PATH"                   = "s3://${aws_s3_bucket.lakehouse.bucket}/dq-reports/bronze-to-silver/"
    "--CATEGORY_REF_PATH"                = "s3://${aws_s3_bucket.lakehouse.bucket}/reference/categories/"
    "--YOUTUBE_API_KEY_SECRET_NAME"      = aws_secretsmanager_secret.youtube_api_key.name
    "--BRONZE_FILE"                      = ""
    "--DQ_REPORT_KEY"                    = ""
    "--MIN_DQ_PASS_RATE"                 = tostring(var.min_dq_pass_rate)
    "--enable-glue-datacatalog"          = "true"
    "--enable-metrics"                   = "true"
    "--enable-job-insights"              = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
  }

  execution_property { max_concurrent_runs = 1 }

  tags = {
    Layer = "silver"
  }
}

resource "aws_glue_job" "silver_to_gold" {
  name         = "${var.project_name}-silver-to-gold"
  role_arn     = aws_iam_role.glue_job_role.arn
  glue_version = "5.1"
  worker_type  = "G.1X"

  number_of_workers = var.glue_worker_count
  timeout           = 30
  max_retries       = 1

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.lakehouse.bucket}/${aws_s3_object.silver_to_gold_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.lakehouse.bucket}/${aws_s3_object.transform_logic_module.key}"
    "--SILVER_PATH"                      = "s3://${aws_s3_bucket.lakehouse.bucket}/silver/youtube/"
    "--GOLD_PATH"                        = "s3://${aws_s3_bucket.lakehouse.bucket}/gold/category_daily_summary/"
    "--REDSHIFT_CONNECTION"              = aws_glue_connection.redshift.name
    "--REDSHIFT_TABLE"                   = "gold.category_daily_summary"
    "--REDSHIFT_STAGE_TABLE"             = "gold.category_daily_summary_stage"
    "--REDSHIFT_TMP_DIR"                 = "s3://${aws_s3_bucket.lakehouse.bucket}/redshift-tmp/"
    "--CATEGORY_REF_PATH"                = "s3://${aws_s3_bucket.lakehouse.bucket}/reference/categories/"
    "--WATERMARK_PATH"                   = "s3://${aws_s3_bucket.lakehouse.bucket}/control/gold_watermark.json"
    "--INCREMENTAL_MODE"                 = tostring(var.gold_incremental_mode)
    "--GOLD_ICEBERG_PATH"                = "s3://${aws_s3_bucket.lakehouse.bucket}/gold-iceberg/category_daily_summary/"
    "--enable-glue-datacatalog"          = "true"
    "--enable-metrics"                   = "true"
    "--enable-job-insights"              = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://${aws_s3_bucket.lakehouse.bucket}/gold-iceberg/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  }

  connections = [aws_glue_connection.redshift.name]

  execution_property { max_concurrent_runs = 1 }

  tags = {
    Layer = "gold"
  }

  depends_on = [aws_redshiftdata_statement.bootstrap_gold_table]
}
