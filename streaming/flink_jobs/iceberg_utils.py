"""Helpers shared by the two Flink jobs.

Both jobs read the same Kafka topic and write to the same Iceberg catalog, so
the table definitions live here instead of being copy-pasted.
"""

import config


def create_kafka_source(t_env, table_name, consumer_group):
    """Register the clickstream Kafka topic as a Flink table."""
    t_env.execute_sql(
        """
        CREATE TABLE {table} (
            event_id    STRING,
            event_type  STRING,
            user_id     STRING,
            session_id  STRING,
            product_id  STRING,
            device_type STRING,
            country     STRING,
            event_time  TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{servers}',
            'properties.group.id' = '{group}',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
        """.format(
            table=table_name,
            topic=config.CLICKSTREAM_TOPIC,
            servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group=consumer_group,
        )
    )


def create_iceberg_catalog(t_env):
    """Connect to the Iceberg REST catalog and make sure the `raw` database exists."""
    t_env.execute_sql(
        """
        CREATE CATALOG lakehouse WITH (
            'type' = 'iceberg',
            'catalog-type' = 'rest',
            'uri' = '{uri}',
            'warehouse' = '{warehouse}',
            's3.endpoint' = '{endpoint}',
            's3.access-key-id' = '{access_key}',
            's3.secret-access-key' = '{secret_key}',
            's3.path-style-access' = 'true'
        )
        """.format(
            uri=config.ICEBERG_CATALOG_URI,
            warehouse=config.ICEBERG_WAREHOUSE,
            endpoint=config.S3_ENDPOINT,
            access_key=config.AWS_ACCESS_KEY_ID,
            secret_key=config.AWS_SECRET_ACCESS_KEY,
        )
    )
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS lakehouse.raw")
