resource "aws_athena_workgroup" "detail_queries" {
  name = "${var.project_name}-detail"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lakehouse.id}/athena-results/"
    }
  }
}

resource "aws_athena_named_query" "video_detail" {
  name        = "${var.project_name}-video-detail"
  workgroup   = aws_athena_workgroup.detail_queries.name
  database    = aws_glue_catalog_database.lakehouse.name
  description = "Per-video Silver detail for drill-down and ad-hoc investigation"
  query       = <<-SQL
    SELECT
      video_id,
      title,
      channel_title,
      category_id,
      region,
      trending_date,
      views,
      likes,
      dislikes,
      comment_count,
      engagement_ratio
    FROM youtube
    ORDER BY views DESC
    LIMIT 500;
  SQL

  depends_on = [aws_glue_crawler.silver_crawler]
}

resource "aws_athena_named_query" "likes_vs_comments" {
  name        = "${var.project_name}-likes-vs-comments"
  workgroup   = aws_athena_workgroup.detail_queries.name
  database    = aws_glue_catalog_database.lakehouse.name
  description = "Per-video likes vs comments detail query"
  query       = <<-SQL
    SELECT title, region, likes, comment_count, views
    FROM youtube
    WHERE views >= 100000
    ORDER BY likes DESC
    LIMIT 5000;
  SQL

  depends_on = [aws_glue_crawler.silver_crawler]
}
