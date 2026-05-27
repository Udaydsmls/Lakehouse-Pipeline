terraform {
  required_providers {
    airbyte = {
      source  = "airbytehq/airbyte"
      version = "~> 0.4"
    }
  }

  required_version = ">= 1.5.0"
}

provider "airbyte" {
  username   = var.airbyte_username
  password   = var.airbyte_password
  server_url = var.airbyte_server_url
}

resource "airbyte_source_postgres" "ecommerce" {
  name         = "ecommerce-postgres"
  workspace_id = var.airbyte_workspace_id

  configuration = {
    source_type = "postgres"
    host        = var.postgres_host
    port        = var.postgres_port
    database    = var.postgres_db
    username    = var.postgres_user
    password    = var.postgres_password

    ssl_mode = {
      mode = "prefer"
    }

    replication_method = {
      method           = "CDC"
      replication_slot = "airbyte_slot"
      publication      = "airbyte_publication"
    }
  }
}

resource "airbyte_source_http_request" "product_catalogue" {
  name         = "product-catalogue-api"
  workspace_id = var.airbyte_workspace_id

  configuration = {
    source_type = "http_request"
    base_url    = var.product_api_base_url
    auth_type   = "bearer_token"
    auth_token  = var.product_api_token
  }
}

resource "airbyte_source_file" "historical_orders" {
  name         = "historical-orders-csv"
  workspace_id = var.airbyte_workspace_id

  configuration = {
    source_type  = "file"
    dataset_name = "historical_orders"
    format       = "csv"

    provider = {
      storage              = "S3"
      bucket               = var.s3_bucket
      path_prefix          = "raw/historical/"
      aws_access_key_id    = var.aws_access_key_id
      aws_secret_access_key = var.aws_secret_access_key
      region_name          = var.aws_region
    }

    reader_options = jsonencode({
      delimiter   = ","
      header      = 0
      parse_dates = ["order_date", "updated_at"]
    })
  }
}

resource "airbyte_destination_snowflake" "raw" {
  name         = "snowflake-raw"
  workspace_id = var.airbyte_workspace_id

  configuration = {
    destination_type = "snowflake"
    account          = var.snowflake_account
    username         = var.snowflake_user
    password         = var.snowflake_password
    database         = var.snowflake_database
    schema           = "RAW"
    warehouse        = var.snowflake_warehouse
    role             = var.snowflake_role

    loading_method = {
      method      = "internal_staging"
      file_format = "PARQUET"
    }
  }
}

resource "airbyte_destination_s3" "raw_parquet" {
  name         = "s3-raw-parquet"
  workspace_id = var.airbyte_workspace_id

  configuration = {
    destination_type  = "s3"
    s3_bucket_name    = var.s3_bucket
    s3_bucket_region  = var.aws_region
    access_key_id     = var.aws_access_key_id
    secret_access_key = var.aws_secret_access_key
    s3_endpoint       = var.s3_endpoint

    format = {
      format_type      = "Parquet"
      compression_codec = "SNAPPY"
    }

    s3_path_format     = "airbyte/{namespace}/{stream_name}/{year}/{month}/{day}/"
    file_name_pattern  = "{date}_{timestamp}_{id}"
  }
}

resource "airbyte_connection" "postgres_to_snowflake" {
  name                 = "postgres-ecommerce-to-snowflake"
  source_id            = airbyte_source_postgres.ecommerce.source_id
  destination_id       = airbyte_destination_snowflake.raw.destination_id
  status               = "active"
  namespace_definition = "destination"
  namespace_format     = "RAW"
  prefix               = "pg_"

  schedule = {
    schedule_type   = "cron"
    cron_expression = "0 */1 * * * ?"
  }

  sync_catalog = {
    streams = [
      {
        stream = {
          name                = "orders"
          source_defined_cursor = true
          default_cursor_field  = ["updated_at"]
          supported_sync_modes  = ["full_refresh", "incremental"]
        }
        config = {
          sync_mode             = "incremental"
          destination_sync_mode = "append_dedup"
          cursor_field          = ["updated_at"]
          primary_key           = [["id"]]
          selected              = true
        }
      },
      {
        stream = {
          name                = "order_items"
          source_defined_cursor = true
          default_cursor_field  = ["updated_at"]
          supported_sync_modes  = ["full_refresh", "incremental"]
        }
        config = {
          sync_mode             = "incremental"
          destination_sync_mode = "append_dedup"
          cursor_field          = ["updated_at"]
          primary_key           = [["id"]]
          selected              = true
        }
      },
      {
        stream = {
          name                = "users"
          source_defined_cursor = true
          default_cursor_field  = ["updated_at"]
          supported_sync_modes  = ["full_refresh", "incremental"]
        }
        config = {
          sync_mode             = "incremental"
          destination_sync_mode = "append_dedup"
          cursor_field          = ["updated_at"]
          primary_key           = [["id"]]
          selected              = true
        }
      },
      {
        stream = {
          name                 = "products"
          supported_sync_modes = ["full_refresh"]
        }
        config = {
          sync_mode             = "full_refresh"
          destination_sync_mode = "overwrite"
          selected              = true
        }
      },
      {
        stream = {
          name                 = "categories"
          supported_sync_modes = ["full_refresh"]
        }
        config = {
          sync_mode             = "full_refresh"
          destination_sync_mode = "overwrite"
          selected              = true
        }
      },
    ]
  }
}

resource "airbyte_connection" "postgres_to_s3" {
  name                 = "postgres-ecommerce-to-s3"
  source_id            = airbyte_source_postgres.ecommerce.source_id
  destination_id       = airbyte_destination_s3.raw_parquet.destination_id
  status               = "active"
  namespace_definition = "custom_format"
  namespace_format     = "postgres"
  prefix               = ""

  schedule = {
    schedule_type   = "cron"
    cron_expression = "0 0 * * * ?"
  }

  sync_catalog = {
    streams = [
      {
        stream = {
          name                = "orders"
          source_defined_cursor = true
          default_cursor_field  = ["updated_at"]
          supported_sync_modes  = ["full_refresh", "incremental"]
        }
        config = {
          sync_mode             = "incremental"
          destination_sync_mode = "append"
          cursor_field          = ["updated_at"]
          selected              = true
        }
      },
      {
        stream = {
          name                = "order_items"
          source_defined_cursor = true
          default_cursor_field  = ["updated_at"]
          supported_sync_modes  = ["full_refresh", "incremental"]
        }
        config = {
          sync_mode             = "incremental"
          destination_sync_mode = "append"
          cursor_field          = ["updated_at"]
          selected              = true
        }
      },
    ]
  }
}

resource "airbyte_connection" "product_api_to_snowflake" {
  name                 = "product-catalogue-api-to-snowflake"
  source_id            = airbyte_source_http_request.product_catalogue.source_id
  destination_id       = airbyte_destination_snowflake.raw.destination_id
  status               = "active"
  namespace_definition = "destination"
  namespace_format     = "RAW"
  prefix               = "api_"

  schedule = {
    schedule_type   = "cron"
    cron_expression = "0 0 6 * * ?"
  }

  sync_catalog = {
    streams = [
      {
        stream = {
          name                 = "products"
          supported_sync_modes = ["full_refresh", "incremental"]
        }
        config = {
          sync_mode             = "incremental"
          destination_sync_mode = "append_dedup"
          cursor_field          = ["updated_at"]
          primary_key           = [["id"]]
          selected              = true
        }
      },
      {
        stream = {
          name                 = "categories"
          supported_sync_modes = ["full_refresh"]
        }
        config = {
          sync_mode             = "full_refresh"
          destination_sync_mode = "overwrite"
          selected              = true
        }
      },
    ]
  }
}

resource "airbyte_connection" "historical_csv_to_s3" {
  name                 = "historical-orders-csv-to-s3"
  source_id            = airbyte_source_file.historical_orders.source_id
  destination_id       = airbyte_destination_s3.raw_parquet.destination_id
  status               = "active"
  namespace_definition = "custom_format"
  namespace_format     = "historical"
  prefix               = ""

  schedule = {
    schedule_type = "manual"
  }

  sync_catalog = {
    streams = [
      {
        stream = {
          name                 = "historical_orders"
          supported_sync_modes = ["full_refresh"]
        }
        config = {
          sync_mode             = "full_refresh"
          destination_sync_mode = "overwrite"
          selected              = true
        }
      },
    ]
  }
}
