output "msk_bootstrap_brokers" {
  description = "TLS bootstrap broker endpoints for the MSK cluster"
  value       = aws_msk_cluster.lakehouse.bootstrap_brokers_tls
}

output "msk_bootstrap_brokers_plaintext" {
  description = "Plaintext bootstrap broker endpoints (only available when client_broker=TLS_PLAINTEXT)"
  value       = aws_msk_cluster.lakehouse.bootstrap_brokers
}

output "s3_raw_bucket_arn" {
  description = "ARN of the S3 raw (Iceberg) bucket"
  value       = aws_s3_bucket.raw.arn
}

output "s3_raw_bucket_name" {
  description = "Name of the S3 raw (Iceberg) bucket"
  value       = aws_s3_bucket.raw.id
}

output "s3_curated_bucket_arn" {
  description = "ARN of the S3 curated (Delta Lake) bucket"
  value       = aws_s3_bucket.curated.arn
}

output "s3_curated_bucket_name" {
  description = "Name of the S3 curated (Delta Lake) bucket"
  value       = aws_s3_bucket.curated.id
}

output "emr_cluster_id" {
  description = "EMR cluster ID for the Spark processing cluster"
  value       = aws_emr_cluster.lakehouse.id
}

output "emr_master_public_dns" {
  description = "Public DNS of the EMR master node (used for Livy and SSH access)"
  value       = aws_emr_cluster.lakehouse.master_public_dns
}

output "snowflake_database_name" {
  description = "Name of the Snowflake lakehouse database"
  value       = snowflake_database.lakehouse.name
}

output "snowflake_warehouse_name" {
  description = "Name of the Snowflake compute warehouse"
  value       = snowflake_warehouse.compute_wh.name
}
