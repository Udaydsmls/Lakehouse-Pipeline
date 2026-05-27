from __future__ import annotations

from pyspark.sql import SparkSession

from batch.spark_jobs.config import SparkJobConfig


def create_spark_session(
    app_name: str,
    config: SparkJobConfig,
    extra_configs: dict | None = None,
) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(config.spark_master)
        # Delta Lake extensions
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Iceberg REST catalog
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", config.iceberg_catalog_uri)
        .config("spark.sql.catalog.lakehouse.warehouse", config.iceberg_warehouse)
        .config(
            "spark.sql.catalog.lakehouse.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(
            "spark.sql.catalog.lakehouse.s3.endpoint",
            config.s3_endpoint,
        )
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        # S3A filesystem
        .config("spark.hadoop.fs.s3a.endpoint", config.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", config.aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", config.aws_secret_access_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Magic committer for high-throughput S3A writes
        .config(
            "spark.hadoop.fs.s3a.committer.name",
            "magic",
        )
        .config(
            "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
            "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory",
        )
        .config(
            "spark.sql.sources.commitProtocolClass",
            "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
        )
        .config(
            "spark.sql.parquet.output.committer.class",
            "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
        )
        # Adaptive query execution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

    return builder.getOrCreate()
