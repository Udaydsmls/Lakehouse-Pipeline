variable "airbyte_username" {
  type        = string
  description = "Airbyte API username"
  default     = "airbyte"
}

variable "airbyte_password" {
  type        = string
  description = "Airbyte API password"
  sensitive   = true
}

variable "airbyte_server_url" {
  type        = string
  description = "Airbyte server base URL"
  default     = "http://localhost:8000"
}

variable "airbyte_workspace_id" {
  type        = string
  description = "Airbyte workspace UUID"
}

variable "postgres_host" {
  type    = string
  default = "localhost"
}

variable "postgres_port" {
  type    = number
  default = 5432
}

variable "postgres_db" {
  type    = string
  default = "ecommerce"
}

variable "postgres_user" {
  type    = string
  default = "postgres"
}

variable "postgres_password" {
  type      = string
  sensitive = true
}

variable "snowflake_account" {
  type = string
}

variable "snowflake_user" {
  type = string
}

variable "snowflake_password" {
  type      = string
  sensitive = true
}

variable "snowflake_database" {
  type    = string
  default = "LAKEHOUSE"
}

variable "snowflake_warehouse" {
  type    = string
  default = "COMPUTE_WH"
}

variable "snowflake_role" {
  type    = string
  default = "SYSADMIN"
}

variable "s3_bucket" {
  type    = string
  default = "lakehouse-raw"
}

variable "s3_endpoint" {
  type    = string
  default = "http://localhost:9000"
}

variable "aws_access_key_id" {
  type      = string
  sensitive = true
}

variable "aws_secret_access_key" {
  type      = string
  sensitive = true
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "product_api_base_url" {
  type = string
}

variable "product_api_token" {
  type      = string
  sensitive = true
}
