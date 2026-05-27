from __future__ import annotations

import io
from typing import Callable

import fastavro

from pyflink.table import StreamTableEnvironment


def create_iceberg_catalog(config, t_env: StreamTableEnvironment) -> None:
    t_env.execute_sql(f"""
        CREATE CATALOG lakehouse WITH (
            'type' = 'iceberg',
            'catalog-type' = 'rest',
            'uri' = '{config.iceberg_catalog_uri}',
            'warehouse' = '{config.iceberg_warehouse}',
            's3.endpoint' = '{config.s3_endpoint}',
            's3.access-key-id' = '{config.aws_access_key_id}',
            's3.secret-access-key' = '{config.aws_secret_access_key}',
            's3.path-style-access' = 'true'
        )
    """)


def ensure_database(t_env: StreamTableEnvironment, catalog_name: str, database: str) -> None:
    t_env.execute_sql(
        f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{database}"
    )


def create_windowed_aggregations_table(t_env: StreamTableEnvironment) -> None:
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.raw.windowed_product_aggregations (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            product_id STRING,
            views_5m BIGINT,
            add_to_cart_5m BIGINT,
            cart_abandonment_rate_5m DOUBLE,
            unique_users_5m BIGINT
        ) PARTITIONED BY (days(window_start))
        WITH (
            'format-version' = '2',
            'write.upsert.enabled' = 'true'
        )
    """)


def create_windowed_user_aggregations_table(t_env: StreamTableEnvironment) -> None:
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.raw.windowed_user_aggregations (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            user_id STRING,
            events_5m BIGINT,
            page_views_5m BIGINT,
            product_views_5m BIGINT,
            searches_5m BIGINT,
            add_to_cart_5m BIGINT,
            purchases_5m BIGINT
        ) PARTITIONED BY (days(window_start))
        WITH (
            'format-version' = '2',
            'write.upsert.enabled' = 'true'
        )
    """)


def create_user_sessions_table(t_env: StreamTableEnvironment) -> None:
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.raw.user_sessions (
            session_id STRING,
            user_id STRING,
            session_start STRING,
            session_end STRING,
            duration_seconds INT,
            pages_viewed INT,
            products_viewed INT,
            searches INT,
            add_to_cart_count INT,
            checkout_started BOOLEAN,
            purchased BOOLEAN,
            outcome STRING,
            device_type STRING,
            country STRING
        ) PARTITIONED BY (bucket(16, user_id))
        WITH (
            'format-version' = '2',
            'write.upsert.enabled' = 'false'
        )
    """)


def avro_deserializer(schema_dict: dict) -> Callable[[bytes], dict]:
    parsed_schema = fastavro.parse_schema(schema_dict)

    def deserialize(data: bytes) -> dict:
        buf = io.BytesIO(data)
        record = fastavro.schemaless_reader(buf, parsed_schema)
        return dict(record)

    return deserialize
