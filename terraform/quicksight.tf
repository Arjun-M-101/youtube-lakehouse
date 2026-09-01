data "aws_iam_policy_document" "quicksight_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["quicksight.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "quicksight_vpc_role" {
  name               = "${var.project_name}-quicksight-vpc-role"
  assume_role_policy = data.aws_iam_policy_document.quicksight_assume_role.json
}

data "aws_iam_policy_document" "quicksight_vpc_policy" {
  statement {
    sid = "QuickSightNetwork"
    actions = [
      "ec2:CreateNetworkInterface",
      "ec2:CreateNetworkInterfacePermission",
      "ec2:ModifyNetworkInterfaceAttribute",
      "ec2:CreateTags",
      "ec2:DeleteNetworkInterface",
      "ec2:DescribeNetworkInterfaces",
      "ec2:DescribeSubnets",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeVpcs",
      "ec2:DescribeRouteTables",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "quicksight_vpc_policy" {
  role   = aws_iam_role.quicksight_vpc_role.id
  name   = "${var.project_name}-quicksight-vpc-policy"
  policy = data.aws_iam_policy_document.quicksight_vpc_policy.json
}

resource "aws_quicksight_vpc_connection" "lakehouse" {
  vpc_connection_id  = "${var.project_name}-vpc-connection"
  name               = "${var.project_name}-vpc-connection"
  aws_account_id     = data.aws_caller_identity.current.account_id
  role_arn            = aws_iam_role.quicksight_vpc_role.arn
  security_group_ids = [aws_security_group.glue_components.id]
  subnet_ids         = [aws_subnet.lakehouse_a.id, aws_subnet.lakehouse_b.id, aws_subnet.lakehouse_c.id]

  depends_on = [aws_iam_role_policy.quicksight_vpc_policy]
}

resource "aws_quicksight_data_source" "redshift" {
  aws_account_id = data.aws_caller_identity.current.account_id
  data_source_id = "${var.project_name}-redshift"
  name           = "${var.project_name} Redshift"
  type           = "REDSHIFT"

  parameters {
    redshift {
      database = aws_redshiftserverless_namespace.lakehouse.db_name
      host     = aws_redshiftserverless_workgroup.lakehouse.endpoint[0].address
      port     = 5439
    }
  }

  credentials {
    credential_pair {
      username = var.redshift_admin_username
      password = var.redshift_admin_password
    }
  }

  vpc_connection_properties {
    vpc_connection_arn = aws_quicksight_vpc_connection.lakehouse.arn
  }

  ssl_properties {
    disable_ssl = false
  }

  permission {
    principal = var.quicksight_user_arn
    actions = [
      "quicksight:DescribeDataSource",
      "quicksight:DescribeDataSourcePermissions",
      "quicksight:PassDataSource",
      "quicksight:UpdateDataSource",
    "quicksight:DeleteDataSource",
      "quicksight:UpdateDataSourcePermissions",
    ]
  }

  depends_on = [aws_redshiftdata_statement.bootstrap_gold_table]
}

resource "aws_quicksight_data_set" "category_daily_performance" {
  aws_account_id = data.aws_caller_identity.current.account_id
  data_set_id    = "${var.project_name}-category-daily-performance"
  name           = "Category Daily Performance"
  import_mode    = "DIRECT_QUERY"

  physical_table_map {
    physical_table_map_id = "category-daily-summary"

    relational_table {
      data_source_arn = aws_quicksight_data_source.redshift.arn
      schema          = "gold"
      name            = "category_daily_summary"

      input_columns {
        name = "category_id"
        type = "INTEGER"
      }
      input_columns {
        name = "category_name"
        type = "STRING"
      }
      input_columns {
        name = "trending_date"
        type = "DATETIME"
      }
      input_columns {
        name = "region"
        type = "STRING"
      }
      input_columns {
        name = "video_count"
        type = "INTEGER"
      }
      input_columns {
        name = "total_views"
        type = "INTEGER"
      }
      input_columns {
        name = "total_likes"
        type = "INTEGER"
      }
      input_columns {
        name = "total_dislikes"
        type = "INTEGER"
      }
      input_columns {
        name = "total_comments"
        type = "INTEGER"
      }
      input_columns {
        name = "avg_views_per_video"
        type = "DECIMAL"
      }
      input_columns {
        name = "avg_engagement_ratio"
        type = "DECIMAL"
      }
    }
  }

  permissions {
    principal = var.quicksight_user_arn
    actions = [
      "quicksight:DescribeDataSet",
      "quicksight:DescribeDataSetPermissions",
      "quicksight:PassDataSet",
      "quicksight:DescribeIngestion",
      "quicksight:ListIngestions",
    ]
  }

  depends_on = [aws_quicksight_data_source.redshift]
}
