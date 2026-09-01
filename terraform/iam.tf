# Service roles are scoped to this project. The deployer identity used by a
# personal sandbox is intentionally separate from runtime roles.

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_job_role" {
  name               = "${var.project_name}-glue-job-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
}

resource "aws_secretsmanager_secret" "youtube_api_key" {
  name        = "${var.project_name}-youtube-data-api-key"
  description = "YouTube Data API v3 key used by the Bronze-to-Silver Glue job"
}

resource "aws_secretsmanager_secret" "redshift_credentials" {
  name        = "${var.project_name}-redshift-credentials"
  description = "Redshift admin credentials used only by the VPC-connected Glue JDBC path and QuickSight"

  recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "redshift_credentials" {
  secret_id = aws_secretsmanager_secret.redshift_credentials.id
  secret_string = jsonencode({
    username = var.redshift_admin_username
    password = var.redshift_admin_password
  })
}

data "aws_iam_policy_document" "glue_job_policy" {
  statement {
    sid = "LakehouseS3Objects"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lakehouse.arn}/*"]
  }

  statement {
    sid       = "LakehouseS3Bucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lakehouse.arn]
  }

  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchCreatePartition",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:UpdatePartition",
      "glue:GetCrawler",
      "glue:GetConnection",
      "glue:GetConnections",
    ]
    resources = ["*"]
  }

  statement {
    sid = "GlueCloudWatchLogs"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams",
    ]
    resources = ["arn:aws:logs:${var.aws_region}:*:log-group:/aws-glue/*"]
  }

  statement {
    sid       = "ReadApiSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.youtube_api_key.arn]
  }

  statement {
    sid       = "ReadRedshiftCredentials"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.redshift_credentials.arn]
  }

  statement {
    sid = "GlueVpcEni"
    actions = [
      "ec2:CreateNetworkInterface",
      "ec2:DeleteNetworkInterface",
      "ec2:DescribeNetworkInterfaces",
      "ec2:DescribeSubnets",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeVpcEndpoints",
      "ec2:DescribeRouteTables",
      "ec2:DescribeVpcs",
      "ec2:CreateTags",
    ]
    resources = ["*"]
  }
  statement {
    sid       = "GlueJobMetrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "glue_job_policy" {
  name   = "${var.project_name}-glue-job-policy"
  role   = aws_iam_role.glue_job_role.id
  policy = data.aws_iam_policy_document.glue_job_policy.json
}

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_trigger_role" {
  name               = "${var.project_name}-lambda-trigger-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

data "aws_iam_policy_document" "lambda_trigger_policy" {
  statement {
    sid       = "StartPipeline"
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.batch_pipeline.arn]
  }

  statement {
    sid = "LambdaLogs"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:aws:logs:${var.aws_region}:*:log-group:/aws/lambda/${var.project_name}-trigger-pipeline:*"]
  }
}

resource "aws_iam_role_policy" "lambda_trigger_policy" {
  name   = "${var.project_name}-lambda-trigger-policy"
  role   = aws_iam_role.lambda_trigger_role.id
  policy = data.aws_iam_policy_document.lambda_trigger_policy.json
}

data "aws_iam_policy_document" "sfn_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn_execution_role" {
  name               = "${var.project_name}-sfn-execution-role"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume_role.json
}

data "aws_iam_policy_document" "sfn_execution_policy" {
  statement {
    sid = "GlueExecution"
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun",
      "glue:StartCrawler",
      "glue:GetCrawler",
    ]
    resources = ["*"]
  }

  statement {
    sid       = "ReadDQReport"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lakehouse.arn}/dq-reports/*"]
  }

  statement {
    sid       = "PublishAlerts"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.pipeline_alerts.arn]
  }

  statement {
    sid = "StateMachineLogs"
    actions = [
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "sfn_execution_policy" {
  name   = "${var.project_name}-sfn-execution-policy"
  role   = aws_iam_role.sfn_execution_role.id
  policy = data.aws_iam_policy_document.sfn_execution_policy.json
}

data "aws_iam_policy_document" "scheduler_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler_role" {
  name               = "${var.project_name}-scheduler-role"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume_role.json
}

data "aws_iam_policy_document" "scheduler_policy" {
  statement {
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.batch_pipeline.arn]
  }
}

resource "aws_iam_role_policy" "scheduler_policy" {
  name   = "${var.project_name}-scheduler-policy"
  role   = aws_iam_role.scheduler_role.id
  policy = data.aws_iam_policy_document.scheduler_policy.json
}

# Redshift assumes this role only to read/write the project's staging prefix.
data "aws_iam_policy_document" "redshift_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "redshift_s3_role" {
  name               = "${var.project_name}-redshift-s3-role"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume_role.json
}

data "aws_iam_policy_document" "redshift_s3_policy" {
  statement {
    actions = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lakehouse.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["redshift-tmp/*"]
    }
  }

  statement {
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lakehouse.arn}/redshift-tmp/*"]
  }
}

resource "aws_iam_role_policy" "redshift_s3_policy" {
  name   = "${var.project_name}-redshift-s3-policy"
  role   = aws_iam_role.redshift_s3_role.id
  policy = data.aws_iam_policy_document.redshift_s3_policy.json
}
