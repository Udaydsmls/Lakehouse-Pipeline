"""Builds the SparkSession used by every batch job.

The config is mostly plumbing: Delta Lake extensions, the Iceberg REST catalog
that Flink writes to, and S3A settings pointed at MinIO.
"""

from pyspark.sql import SparkSession

import config


def create_spark_session(app_name):
    return (
        SparkSession.builder.appName(app_name)
        .master(config.SPARK_MASTER)
        # Delta Lake (curated zone)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Iceberg REST catalog (raw zone, written by Flink)
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", config.ICEBERG_CATALOG_URI)
        .config("spark.sql.catalog.lakehouse.warehouse", config.ICEBERG_WAREHOUSE)
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", config.S3_ENDPOINT)
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        # MinIO via the S3A filesystem
        .config("spark.hadoop.fs.s3a.endpoint", config.S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", config.AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", config.AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def read_postgres(spark, table):
    """Read a table from the source Postgres database."""
    return spark.read.jdbc(
        url=config.JDBC_URL,
        table=table,
        properties=config.JDBC_PROPERTIES,
    )


def write_postgres(df, table):
    """Overwrite a table in the Postgres `analytics` schema so dbt can read it."""
    df.write.jdbc(
        url=config.JDBC_URL,
        table=table,
        mode="overwrite",
        properties=config.JDBC_PROPERTIES,
    )
