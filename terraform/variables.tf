variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "ap-south-1"
}

variable "environment" {
  description = "Deployment environment name"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Short name used as a resource-naming prefix"
  type        = string
  default     = "youtube-lakehouse"
}

variable "redshift_admin_username" {
  description = "Admin username for the Redshift Serverless namespace"
  type        = string
  default     = "lakehouse_admin"
}

variable "redshift_admin_password" {
  description = "Strong Redshift admin password. Supply with TF_VAR_redshift_admin_password; never commit it."
  type        = string
  sensitive   = true
}

variable "redshift_publicly_accessible" {
  type    = bool
  default = false # QuickSight-safe default; only true transiently for dbt
}

variable "alert_email" {
  description = "Email address for SNS pipeline failure alerts"
  type        = string
}

variable "quicksight_user_arn" {
  description = "QuickSight user ARN allowed to access the project dashboard resources"
  type        = string
}

variable "min_dq_pass_rate" {
  description = "Minimum fraction of Bronze rows that must pass validation before Gold is built"
  type        = number
  default     = 0.95

  validation {
    condition     = var.min_dq_pass_rate > 0 && var.min_dq_pass_rate <= 1
    error_message = "min_dq_pass_rate must be greater than 0 and no greater than 1."
  }
}

variable "pipeline_schedule_expression" {
  description = "EventBridge Scheduler expression used as the daily backstop"
  type        = string
  default     = "cron(0 6 * * ? *)"
}

variable "glue_worker_count" {
  description = "Number of G.1X workers used by each Glue ETL job"
  type        = number
  default     = 2

  validation {
    condition     = var.glue_worker_count >= 2
    error_message = "glue_worker_count must be at least 2 for this project."
  }
}
