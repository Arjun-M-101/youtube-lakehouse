resource "aws_redshiftserverless_namespace" "lakehouse" {
  namespace_name      = "${var.project_name}-ns"
  db_name             = "youtube_lakehouse"
  admin_username      = var.redshift_admin_username
  admin_user_password = var.redshift_admin_password
  iam_roles           = [aws_iam_role.redshift_s3_role.arn]
}

resource "aws_redshiftserverless_workgroup" "lakehouse" {
  workgroup_name = "${var.project_name}-wg"
  namespace_name = aws_redshiftserverless_namespace.lakehouse.namespace_name
  base_capacity  = 8
  max_capacity   = 16

  subnet_ids = [
    aws_subnet.lakehouse_a.id,
    aws_subnet.lakehouse_b.id,
    aws_subnet.lakehouse_c.id,
  ]
  security_group_ids   = [aws_security_group.redshift_access.id]
  publicly_accessible  = false
  enhanced_vpc_routing = true
}

# Creates the schemas/table shape before the first pipeline run. This makes
# the QuickSight dataset deterministic and also gives the Glue JDBC load a
# stable target. The Silver->Gold job then atomically refreshes the table.
resource "aws_redshiftdata_statement" "bootstrap_gold_schema" {
  workgroup_name = aws_redshiftserverless_workgroup.lakehouse.workgroup_name
  database       = aws_redshiftserverless_namespace.lakehouse.db_name
  secret_arn     = aws_secretsmanager_secret.redshift_credentials.arn
  statement_name = "${var.project_name}-bootstrap-gold-schema"
  sql            = "CREATE SCHEMA IF NOT EXISTS gold;"

  depends_on = [
    aws_secretsmanager_secret_version.redshift_credentials,
    aws_iam_role_policy.redshift_s3_policy,
  ]
}

resource "aws_redshiftdata_statement" "bootstrap_gold_table" {
  workgroup_name = aws_redshiftserverless_workgroup.lakehouse.workgroup_name
  database       = aws_redshiftserverless_namespace.lakehouse.db_name
  secret_arn     = aws_secretsmanager_secret.redshift_credentials.arn
  statement_name = "${var.project_name}-bootstrap-gold-table"
  sql            = <<-SQL
    CREATE TABLE IF NOT EXISTS gold.category_daily_summary (
      category_id INTEGER,
      category_name VARCHAR(256),
      trending_date DATE,
      region VARCHAR(8),
      video_count BIGINT,
      total_views BIGINT,
      total_likes BIGINT,
      total_dislikes BIGINT,
      total_comments BIGINT,
      avg_views_per_video DOUBLE PRECISION,
      avg_engagement_ratio DOUBLE PRECISION
    );
  SQL

  depends_on = [aws_redshiftdata_statement.bootstrap_gold_schema]
}
