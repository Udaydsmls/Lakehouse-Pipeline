variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (prod, staging, dev)"
  type        = string
  default     = "prod"
}

variable "project_name" {
  description = "Project name used as a prefix for all resources"
  type        = string
  default     = "lakehouse"
}

variable "kafka_instance_type" {
  description = "MSK broker instance type"
  type        = string
  default     = "kafka.m5.large"
}

variable "kafka_number_of_broker_nodes" {
  description = "Number of MSK broker nodes (must be a multiple of AZs used)"
  type        = number
  default     = 3
}

variable "kafka_kafka_version" {
  description = "Apache Kafka version for the MSK cluster"
  type        = string
  default     = "3.5.1"
}

variable "emr_master_instance_type" {
  description = "EC2 instance type for the EMR master node"
  type        = string
  default     = "m5.xlarge"
}

variable "emr_core_instance_type" {
  description = "EC2 instance type for EMR core nodes"
  type        = string
  default     = "m5.2xlarge"
}

variable "emr_core_instance_count" {
  description = "Number of EMR core nodes"
  type        = number
  default     = 3
}

variable "emr_release_label" {
  description = "EMR release label"
  type        = string
  default     = "emr-6.15.0"
}

variable "s3_raw_bucket_name" {
  description = "Name of the S3 bucket for raw (Iceberg) data"
  type        = string
}

variable "s3_curated_bucket_name" {
  description = "Name of the S3 bucket for curated (Delta Lake) data"
  type        = string
}

variable "s3_logs_bucket_name" {
  description = "Name of the S3 bucket for EMR and application logs"
  type        = string
}

variable "snowflake_account" {
  description = "Snowflake account identifier (org-account format)"
  type        = string
  sensitive   = true
}

variable "snowflake_user" {
  description = "Snowflake username for the Terraform service account"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake password for the Terraform service account"
  type        = string
  sensitive   = true
}

variable "snowflake_role" {
  description = "Snowflake role used by Terraform (must own the objects it creates)"
  type        = string
  default     = "SYSADMIN"
}

variable "vpc_id" {
  description = "VPC ID where MSK, EMR, and supporting resources are deployed"
  type        = string
}

variable "subnet_ids" {
  description = "List of subnet IDs for MSK broker placement and EMR cluster"
  type        = list(string)
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to connect to MSK and other internal services"
  type        = list(string)
  default     = ["10.0.0.0/8"]
}
