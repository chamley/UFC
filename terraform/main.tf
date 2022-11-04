terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }

  required_version = ">= 1.2.0"
}



resource "aws_redshift_cluster" "redshift-prod-cluster" {
  cluster_identifier = "ufc-main"
  database_name      = "dev"
  node_type          = "dc2.large"
  master_username    = var.master_username
  master_password    = var.master_password
}
