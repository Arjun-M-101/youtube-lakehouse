data "archive_file" "trigger_lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/lambda/trigger_pipeline.py"
  output_path = "${path.module}/trigger_pipeline.zip"
}

resource "aws_lambda_function" "trigger_pipeline" {
  function_name = "${var.project_name}-trigger-pipeline"
  role          = aws_iam_role.lambda_trigger_role.arn
  handler       = "trigger_pipeline.lambda_handler"
  runtime       = "python3.12"
  timeout       = 10
  memory_size   = 128

  filename         = data.archive_file.trigger_lambda_zip.output_path
  source_code_hash = data.archive_file.trigger_lambda_zip.output_base64sha256

  environment {
    variables = {
      STATE_MACHINE_ARN = aws_sfn_state_machine.batch_pipeline.arn
    }
  }
}

resource "aws_cloudwatch_log_group" "trigger_lambda" {
  name              = "/aws/lambda/${aws_lambda_function.trigger_pipeline.function_name}"
  retention_in_days = 30
}

resource "aws_lambda_permission" "allow_s3_invoke" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.trigger_pipeline.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.lakehouse.arn
}

resource "aws_s3_bucket_notification" "bronze_upload_trigger" {
  bucket = aws_s3_bucket.lakehouse.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.trigger_pipeline.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "bronze/youtube/"
    filter_suffix       = ".csv"
  }

  depends_on = [aws_lambda_permission.allow_s3_invoke]
}
