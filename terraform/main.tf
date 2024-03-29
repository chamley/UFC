## Todo:
## ECR + LAMBDA


terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }
  required_version = ">= 1.2.0"
}

# imported
resource "aws_redshift_cluster" "redshift-prod-cluster" {
  cluster_identifier = "ufc-main"
  database_name      = "dev"
  node_type          = "dc2.large"
  master_username    = var.master_username
  master_password    = var.master_password
}

# imported
resource "aws_s3_bucket" "stage-layer-one" {
  bucket = var.stage-layer-one-bucket
}

# imported
resource "aws_s3_bucket" "stage-layer-two" {
  bucket = var.stage-layer-two-bucket
}

# imported
resource "aws_s3_bucket" "mwaa-bucket" {
  bucket = var.mwaa-dag-bucket
}

# imported
resource "aws_s3_bucket" "ufc-config-bucket" {
  bucket = var.ufc-config-files-bucket
}
# imported
resource "aws_iam_group" "developers" {
  name = "dev"
}


