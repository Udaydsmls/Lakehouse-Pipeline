"""Daily batch job: Iceberg raw zone -> Delta Lake curated zone -> Postgres.

Flink drops raw session and 5-minute product rows into Iceberg. This job cleans
them up, rolls the 5-minute rows into daily product numbers, and stores the
result as Delta tables on MinIO. The same tables are then copied into the
Postgres `analytics` schema, which is what dbt and Metabase read from.

Usage:
    python batch/spark_jobs/raw_to_curated.py 2026-09-17
"""

import sys
from datetime import date, timedelta

from pyspark.sql import functions as F

import config
from spark_session import create_spark_session, read_postgres, write_postgres


def load_sessions(spark, processing_date):
    """Read one day of sessions from Iceberg and drop duplicate session ids."""
    return (
        spark.read.format("iceberg")
        .load("lakehouse.raw.user_sessions")
        .filter(F.col("session_date") == processing_date)
        .dropDuplicates(["session_id"])
    )


def build_product_performance(spark, processing_date):
    """Daily views / carts / purchases per product, plus a conversion rate."""
    metrics = (
        spark.read.format("iceberg")
        .load("lakehouse.raw.product_metrics_5min")
        .filter(F.col("event_date") == processing_date)
    )

    daily = metrics.groupBy("product_id").agg(
        F.sum("views").alias("total_views"),
        F.sum("add_to_cart").alias("total_add_to_cart"),
        F.sum("purchases").alias("total_purchases"),
    )

    products = read_postgres(spark, "products").select(
        "product_id", "name", "category", "subcategory", "brand"
    )

    return (
        daily.join(products, on="product_id", how="left")
        .withColumn(
            "conversion_rate",
            F.when(F.col("total_views") > 0, F.col("total_purchases") / F.col("total_views")).otherwise(0.0),
        )
        .withColumn(
            "cart_abandonment_rate",
            F.when(
                F.col("total_add_to_cart") > 0,
                1 - (F.col("total_purchases") / F.col("total_add_to_cart")),
            ).otherwise(0.0),
        )
        .withColumn("date", F.lit(processing_date).cast("date"))
        .withColumnRenamed("name", "product_name")
    )


def write_delta_partition(df, table_name, partition_column, processing_date):
    """Write one day of data, replacing that day if the job is re-run."""
    path = "%s/%s" % (config.DELTA_WAREHOUSE, table_name)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", "%s = '%s'" % (partition_column, processing_date))
        .partitionBy(partition_column)
        .save(path)
    )
    return path


def copy_to_postgres(spark, delta_path, table_name):
    """Reload the full Delta table into Postgres so dbt sees all history."""
    df = spark.read.format("delta").load(delta_path)
    write_postgres(df, "analytics.%s" % table_name)
    print("Loaded %d rows into analytics.%s" % (df.count(), table_name))


def main():
    if len(sys.argv) > 1:
        processing_date = sys.argv[1]
    else:
        processing_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    spark = create_spark_session("raw-to-curated")
    print("Processing %s" % processing_date)

    try:
        sessions = load_sessions(spark, processing_date)
        if sessions.rdd.isEmpty():
            print("No sessions found for %s, nothing to do." % processing_date)
            return

        sessions_path = write_delta_partition(
            sessions, "user_sessions", "session_date", processing_date
        )
        copy_to_postgres(spark, sessions_path, "user_sessions")

        product_perf = build_product_performance(spark, processing_date)
        product_perf_path = write_delta_partition(
            product_perf, "product_performance", "date", processing_date
        )
        copy_to_postgres(spark, product_perf_path, "product_performance")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
