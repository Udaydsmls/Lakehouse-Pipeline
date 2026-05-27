resource "snowflake_database" "lakehouse" {
  name    = "LAKEHOUSE"
  comment = "Central lakehouse database for e-commerce analytics — raw zone (Iceberg via external tables), curated Delta parquet, and dbt mart models"
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.lakehouse.name
  name     = "RAW"
  comment  = "External tables pointing to Iceberg/Parquet files in the S3 raw zone"
}

resource "snowflake_schema" "staging" {
  database = snowflake_database.lakehouse.name
  name     = "STAGING"
  comment  = "dbt staging models: light cleaning and type casting of raw sources"
}

resource "snowflake_schema" "marts" {
  database = snowflake_database.lakehouse.name
  name     = "MARTS"
  comment  = "dbt mart models: business-facing dimensional and fact tables"
}

resource "snowflake_schema" "external" {
  database = snowflake_database.lakehouse.name
  name     = "EXTERNAL"
  comment  = "External tables over Delta Parquet files in the curated S3 zone"
}

resource "snowflake_warehouse" "compute_wh" {
  name                = "COMPUTE_WH"
  warehouse_size      = "X-SMALL"
  auto_suspend        = 300
  auto_resume         = true
  max_cluster_count   = 3
  min_cluster_count   = 1
  scaling_policy      = "ECONOMY"
  comment             = "Default compute warehouse for dbt transformations and ad-hoc queries"
}

resource "snowflake_role" "reporter" {
  name    = "REPORTER"
  comment = "Read-only role for BI tools (Metabase) and analysts who should not modify data"
}

resource "snowflake_grant_privileges_to_role" "reporter_usage_db" {
  role_name  = snowflake_role.reporter.name
  privileges = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.lakehouse.name
  }
}

resource "snowflake_grant_privileges_to_role" "reporter_usage_marts" {
  role_name  = snowflake_role.reporter.name
  privileges = ["USAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.lakehouse.name}\".\"${snowflake_schema.marts.name}\""
  }
}

resource "snowflake_grant_privileges_to_role" "reporter_select_marts" {
  role_name  = snowflake_role.reporter.name
  privileges = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.lakehouse.name}\".\"${snowflake_schema.marts.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_role" "reporter_usage_wh" {
  role_name  = snowflake_role.reporter.name
  privileges = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.compute_wh.name
  }
}

resource "snowflake_storage_integration" "s3_int" {
  name    = "S3_LAKEHOUSE_INT"
  type    = "EXTERNAL_STAGE"
  comment = "Storage integration for reading Parquet/Iceberg files from S3 raw and curated buckets"

  storage_provider     = "S3"
  enabled              = true
  storage_allowed_locations = [
    "s3://${var.s3_raw_bucket_name}/",
    "s3://${var.s3_curated_bucket_name}/",
  ]
}

resource "snowflake_stage" "s3_raw_stage" {
  name                = "S3_RAW_STAGE"
  database            = snowflake_database.lakehouse.name
  schema              = snowflake_schema.raw.name
  url                 = "s3://${var.s3_raw_bucket_name}/"
  storage_integration = snowflake_storage_integration.s3_int.name
  comment             = "External stage pointing to the S3 raw zone; used by external tables in the EXTERNAL schema"

  file_format = "TYPE = PARQUET SNAPPY_COMPRESSION = TRUE"
}

resource "snowflake_external_table" "user_features" {
  database    = snowflake_database.lakehouse.name
  schema      = snowflake_schema.external.name
  name        = "USER_FEATURES"
  comment     = "Delta Lake Parquet user feature table produced by the Spark batch user_features job"
  location    = "@${snowflake_database.lakehouse.name}.${snowflake_schema.raw.name}.S3_RAW_STAGE/curated/user_features/"
  file_format = "TYPE = PARQUET SNAPPY_COMPRESSION = TRUE"

  column {
    name = "user_id"
    type = "VARCHAR"
    as   = "($1:user_id::VARCHAR)"
  }
  column {
    name = "total_sessions"
    type = "NUMBER"
    as   = "($1:total_sessions::NUMBER)"
  }
  column {
    name = "total_orders"
    type = "NUMBER"
    as   = "($1:total_orders::NUMBER)"
  }
  column {
    name = "total_spend"
    type = "FLOAT"
    as   = "($1:total_spend::FLOAT)"
  }
  column {
    name = "avg_order_value"
    type = "FLOAT"
    as   = "($1:avg_order_value::FLOAT)"
  }
  column {
    name = "days_since_last_order"
    type = "NUMBER"
    as   = "($1:days_since_last_order::NUMBER)"
  }
  column {
    name = "rfm_segment"
    type = "VARCHAR"
    as   = "($1:rfm_segment::VARCHAR)"
  }
  column {
    name = "ltv_estimate"
    type = "FLOAT"
    as   = "($1:ltv_estimate::FLOAT)"
  }
  column {
    name = "computed_at"
    type = "TIMESTAMP_NTZ"
    as   = "($1:computed_at::TIMESTAMP_NTZ)"
  }
}

resource "snowflake_external_table" "product_performance" {
  database    = snowflake_database.lakehouse.name
  schema      = snowflake_schema.external.name
  name        = "PRODUCT_PERFORMANCE"
  comment     = "Delta Lake Parquet product performance aggregations produced by the Spark batch raw_to_curated job"
  location    = "@${snowflake_database.lakehouse.name}.${snowflake_schema.raw.name}.S3_RAW_STAGE/curated/product_performance/"
  file_format = "TYPE = PARQUET SNAPPY_COMPRESSION = TRUE"

  column {
    name = "product_id"
    type = "VARCHAR"
    as   = "($1:product_id::VARCHAR)"
  }
  column {
    name = "date"
    type = "DATE"
    as   = "($1:date::DATE)"
  }
  column {
    name = "total_views"
    type = "NUMBER"
    as   = "($1:total_views::NUMBER)"
  }
  column {
    name = "total_add_to_cart"
    type = "NUMBER"
    as   = "($1:total_add_to_cart::NUMBER)"
  }
  column {
    name = "total_purchases"
    type = "NUMBER"
    as   = "($1:total_purchases::NUMBER)"
  }
  column {
    name = "unique_users"
    type = "NUMBER"
    as   = "($1:unique_users::NUMBER)"
  }
  column {
    name = "revenue"
    type = "FLOAT"
    as   = "($1:revenue::FLOAT)"
  }
}
