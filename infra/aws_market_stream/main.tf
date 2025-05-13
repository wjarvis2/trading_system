################################################################################
# AWS Market‑Data Streaming Stack – Terraform skeleton                          #
#                                                                              #
#   ▸ VPC (private RDS, public Fargate)                                         #
#   ▸ ECS Cluster + 2 Services (ib-gateway, ib-collector)                       #
#   ▸ SQS Standard queue                                                       #
#   ▸ Secrets Manager creds                                                    #
#   ▸ RDS PostgreSQL (Multi‑AZ)                                                #
#   ▸ Lambda loader triggered by SQS                                           #
################################################################################

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.50.0"
    }
  }
  backend "s3" {
    bucket = "my-marketdata-tfstate-1747136145"   
    key    = "market-data/terraform.tfstate"
    region = "us-east-1"                 
  }
}

provider "aws" {
  region              = var.aws_region
  profile             = var.aws_profile   # set in ~/.aws/credentials
  default_tags = {
    Project = "MarketDataStream"
  }
}

###############################
#   ▸ Variables              #
###############################

variable "aws_region"   { type = string  default = "us-east-1" }
variable "aws_profile"  { type = string  default = "default" }
variable "db_password"  { type = string  sensitive = true }
variable "ib_user"      { type = string  sensitive = true }
variable "ib_password"  { type = string  sensitive = true }
variable "ib_host_port" {
  description = "IB Gateway will expose port 4001 inside the task; leave default"
  type        = number
  default     = 4001
}

###############################
#   ▸ VPC & subnets          #
###############################

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "5.5.0"

  name                 = "market-data-vpc"
  cidr                 = "10.80.0.0/16"
  azs                  = ["${var.aws_region}a", "${var.aws_region}b"]
  public_subnets       = ["10.80.1.0/24", "10.80.2.0/24"]
  private_subnets      = ["10.80.11.0/24", "10.80.12.0/24"]
  enable_nat_gateway   = true
  single_nat_gateway   = true
  enable_dns_hostnames = true
}

#########################################
#   ▸ ECS cluster & task execution IAM  #
#########################################

resource "aws_ecs_cluster" "md_cluster" {
  name = "md-stream-cluster"
}

module "ecs_task_iam" {
  source  = "cloudposse/iam-role/aws"
  version = "0.18.0"

  name                 = "md-ecs-task"
  assume_role_policy   = data.aws_iam_policy_document.ecs_assume.json
  policies             = [aws_iam_policy.task_exec.arn]
}

data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_policy" "task_exec" {
  name   = "md-task-exec"
  policy = data.aws_iam_policy_document.task_exec.json
}

data "aws_iam_policy_document" "task_exec" {
  statement {
    actions   = ["sqs:SendMessage", "secretsmanager:GetSecretValue", "logs:*", "ssm:GetParameters"]
    resources = ["*"]
  }
}

###############################
#   ▸ Secrets Manager        #
###############################

resource "aws_secretsmanager_secret" "ib_creds" {
  name = "ibkr-creds"
}

resource "aws_secretsmanager_secret_version" "ib_creds_ver" {
  secret_id     = aws_secretsmanager_secret.ib_creds.id
  secret_string = jsonencode({
    USERNAME = var.ib_user,
    PASSWORD = var.ib_password
  })
}

#################################
#   ▸ SQS tick buffer queue     #
#################################

resource "aws_sqs_queue" "tick_buffer" {
  name                      = "md-tick-buffer"
  message_retention_seconds = 1209600   # 14 days
}

#######################
#   ▸ RDS Postgres    #
#######################

module "db" {
  source  = "terraform-aws-modules/rds/aws"
  version = "6.5.0"

  identifier = "md-timescaledb"
  engine     = "postgres"
  engine_version         = "15.4"
  instance_class         = "db.t3.medium"
  allocated_storage      = 100
  password               = var.db_password
  username               = "energy"
  db_subnet_group_name   = module.vpc.database_subnet_group
  vpc_security_group_ids = [module.vpc.default_security_group_id]
  multi_az               = true
  skip_final_snapshot    = true
}

##############################
#   ▸ Lambda loader         #
##############################

resource "aws_iam_role" "lambda_role" {
  name               = "md-sqs-loader-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "lambda_policy" {
  role   = aws_iam_role.lambda_role.id
  policy = data.aws_iam_policy_document.lambda_policy.json
}

data "aws_iam_policy_document" "lambda_policy" {
  statement {
    actions   = ["logs:*", "sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "secretsmanager:GetSecretValue", "rds-db:connect"]
    resources = ["*"]
  }
}

resource "aws_lambda_function" "sqs_loader" {
  function_name = "md-sqs-loader"
  runtime       = "python3.12"
  handler       = "loader.lambda_handler"
  role          = aws_iam_role.lambda_role.arn

  filename         = "lambda_loader.zip"   # build artifact (see README)
  source_code_hash = filebase64sha256("lambda_loader.zip")

  environment {
    variables = {
      PG_DSN   = module.db.db_instance_endpoint
      SQS_URL  = aws_sqs_queue.tick_buffer.id
    }
  }

  timeout = 60
  memory_size = 512
}

resource "aws_lambda_event_source_mapping" "sqs_trigger" {
  event_source_arn = aws_sqs_queue.tick_buffer.arn
  function_name    = aws_lambda_function.sqs_loader.arn
  batch_size       = 10
  maximum_batching_window_in_seconds = 10
}

##############################
#   ▸ Outputs               #
##############################

output "db_endpoint" {
  value = module.db.db_instance_endpoint
}

output "sqs_url" {
  value = aws_sqs_queue.tick_buffer.id
}
