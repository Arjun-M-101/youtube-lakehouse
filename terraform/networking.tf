# Private networking for Redshift Serverless and the VPC-connected Glue ETL job.
# There is intentionally no NAT Gateway: S3 is reachable through a free gateway
# endpoint, and the Glue runtime can reach Secrets Manager/Logs/STS through
# interface endpoints.

data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "lakehouse" {
  cidr_block           = "10.20.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
}

resource "aws_subnet" "lakehouse_a" {
  vpc_id            = aws_vpc.lakehouse.id
  cidr_block        = "10.20.1.0/24"
  availability_zone = data.aws_availability_zones.available.names[0]
}

resource "aws_subnet" "lakehouse_b" {
  vpc_id            = aws_vpc.lakehouse.id
  cidr_block        = "10.20.2.0/24"
  availability_zone = data.aws_availability_zones.available.names[1]
}

resource "aws_subnet" "lakehouse_c" {
  vpc_id            = aws_vpc.lakehouse.id
  cidr_block        = "10.20.3.0/24"
  availability_zone = data.aws_availability_zones.available.names[2]
}

resource "aws_route_table" "lakehouse" {
  vpc_id = aws_vpc.lakehouse.id
}

resource "aws_route_table_association" "a" {
  subnet_id      = aws_subnet.lakehouse_a.id
  route_table_id = aws_route_table.lakehouse.id
}

resource "aws_route_table_association" "b" {
  subnet_id      = aws_subnet.lakehouse_b.id
  route_table_id = aws_route_table.lakehouse.id
}

resource "aws_route_table_association" "c" {
  subnet_id      = aws_subnet.lakehouse_c.id
  route_table_id = aws_route_table.lakehouse.id
}

resource "aws_vpc_dhcp_options" "lakehouse" {
  domain_name         = "${var.aws_region}.compute.internal"
  domain_name_servers = ["AmazonProvidedDNS"]
}

resource "aws_vpc_dhcp_options_association" "lakehouse" {
  vpc_id          = aws_vpc.lakehouse.id
  dhcp_options_id = aws_vpc_dhcp_options.lakehouse.id
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.lakehouse.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.lakehouse.id]
}

resource "aws_internet_gateway" "lakehouse" {
  vpc_id = aws_vpc.lakehouse.id

  tags = {
    Name = "${var.project_name}-igw"
  }
}

resource "aws_route" "internet_access" {
  route_table_id         = aws_route_table.lakehouse.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id              = aws_internet_gateway.lakehouse.id
}

resource "aws_security_group" "glue_endpoints" {
  name_prefix = "${var.project_name}-endpoints-"
  description = "Interface endpoints for the lakehouse VPC"
  vpc_id      = aws_vpc.lakehouse.id

  ingress {
    description     = "HTTPS from Glue/analytics components"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [aws_security_group.glue_components.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle { create_before_destroy = true }
}

resource "aws_security_group" "glue_components" {
  name_prefix = "${var.project_name}-glue-"
  description = "Security group for the VPC-connected Glue ETL job"
  vpc_id      = aws_vpc.lakehouse.id

  ingress {
    description     = "Glue workers communicate with each other"
    from_port       = 0
    to_port         = 65535
    protocol        = "tcp"
    self            = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle { create_before_destroy = true }
}

resource "aws_security_group" "redshift_access" {
  name_prefix = "${var.project_name}-redshift-"
  description = "Redshift Serverless access from Glue and approved VPC clients"
  vpc_id      = aws_vpc.lakehouse.id

  ingress {
    description     = "JDBC from the Glue job security group"
    from_port       = 5439
    to_port         = 5439
    protocol        = "tcp"
    security_groups = [aws_security_group.glue_components.id]
  }

  ingress {
    description = "TEMP: local dbt access for Arjun laptop, remove before final delivery"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["49.204.118.177/32"]
  }

  ingress {
    description = "QuickSight VPC connection fallback (public path)"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle { create_before_destroy = true }
}

resource "aws_vpc_endpoint" "secretsmanager" {
  vpc_id              = aws_vpc.lakehouse.id
  service_name        = "com.amazonaws.${var.aws_region}.secretsmanager"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.lakehouse_a.id, aws_subnet.lakehouse_b.id, aws_subnet.lakehouse_c.id]
  security_group_ids  = [aws_security_group.glue_endpoints.id]
  private_dns_enabled = true
}

resource "aws_vpc_endpoint" "logs" {
  vpc_id              = aws_vpc.lakehouse.id
  service_name        = "com.amazonaws.${var.aws_region}.logs"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.lakehouse_a.id, aws_subnet.lakehouse_b.id, aws_subnet.lakehouse_c.id]
  security_group_ids  = [aws_security_group.glue_endpoints.id]
  private_dns_enabled = true
}

resource "aws_vpc_endpoint" "sts" {
  vpc_id              = aws_vpc.lakehouse.id
  service_name        = "com.amazonaws.${var.aws_region}.sts"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.lakehouse_a.id, aws_subnet.lakehouse_b.id, aws_subnet.lakehouse_c.id]
  security_group_ids  = [aws_security_group.glue_endpoints.id]
  private_dns_enabled = true
}
