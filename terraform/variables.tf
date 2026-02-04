variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "ecommerce-analytics"
}

variable "s3_bucket_prefix" {
  description = "Prefix for S3 bucket names"
  type        = string
  default     = "ddb-analytics"
}





