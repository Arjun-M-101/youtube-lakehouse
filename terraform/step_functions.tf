resource "aws_sfn_state_machine" "batch_pipeline" {
  name     = "${var.project_name}-batch-pipeline"
  role_arn = aws_iam_role.sfn_execution_role.arn

  definition = templatefile("${path.module}/../step_functions/state_machine.json", {
    bronze_to_silver_job_name = aws_glue_job.bronze_to_silver.name
    silver_to_gold_job_name   = aws_glue_job.silver_to_gold.name
    silver_crawler_name       = aws_glue_crawler.silver_crawler.name
    sns_topic_arn             = aws_sns_topic.pipeline_alerts.arn
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn_logs.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  depends_on = [aws_cloudwatch_log_resource_policy.sfn_log_delivery]
}

resource "aws_cloudwatch_log_group" "sfn_logs" {
  name              = "/aws/vendedlogs/states/${var.project_name}-batch-pipeline"
  retention_in_days = 30
}

data "aws_iam_policy_document" "sfn_log_delivery" {
  statement {
    sid    = "SfnLogDelivery"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["delivery.logs.amazonaws.com"]
    }

    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]

    resources = ["${aws_cloudwatch_log_group.sfn_logs.arn}:*"]
  }
}

resource "aws_cloudwatch_log_resource_policy" "sfn_log_delivery" {
  policy_name     = "${var.project_name}-sfn-log-delivery"
  policy_document = data.aws_iam_policy_document.sfn_log_delivery.json
}
