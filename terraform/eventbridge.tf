resource "aws_scheduler_schedule" "daily_backstop" {
  name                         = "${var.project_name}-daily-backstop"
  schedule_expression          = var.pipeline_schedule_expression
  schedule_expression_timezone = "UTC"
  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_sfn_state_machine.batch_pipeline.arn
    role_arn = aws_iam_role.scheduler_role.arn
    input = jsonencode({
      source       = "eventbridge-daily-backstop"
      triggeredKey = ""
      bucket       = aws_s3_bucket.lakehouse.bucket
    })
  }

  depends_on = [aws_iam_role_policy.scheduler_policy]
}
