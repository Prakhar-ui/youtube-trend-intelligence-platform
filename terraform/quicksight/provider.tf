terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket       = "yt-terraform-state-prakhar"
    key          = "quicksight/terraform.tfstate"
    region       = "ap-south-1"
    use_lockfile = true
    encrypt      = true
  }
}

provider "aws" {
  region = "ap-south-1"
}

# Dynamically resolve account ID and region for portable ARN construction
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
