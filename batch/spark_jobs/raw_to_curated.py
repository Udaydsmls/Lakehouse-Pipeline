from __future__ import annotations

import sys
import logging
from datetime import date, timedelta

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from batch.spark_jobs.config import SparkJobConfig
from batch.spark_jobs.spark_session import create_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def _read_postgres(spark: SparkSession, config: SparkJobConfig, table: str) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", config.jdbc_url)
        .option("dbtable", table)
        .option("user", config.postgres_user)
        .option("password", config.postgres_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def build_product_performance(
    spark: SparkSession,
    config: SparkJobConfig,
    windowed_agg: DataFrame,
    user_sessions: DataFrame,
    products_dim: DataFrame,
    processing_date: str,
) -> None:
    purchases_per_product = (
        user_sessions.filter(F.col("outcome") == "purchase")
        .groupBy("product_id")
        .agg(F.count("*").alias("purchase_sessions"))
    )

    product_perf = (
        windowed_agg.join(products_dim, on="product_id", how="left")
        .join(purchases_per_product, on="product_id", how="left")
        .groupBy(
            "product_id",
            F.col("name").alias("product_name"),
            "category",
            "subcategory",
            "brand",
        )
        .agg(
            F.sum("view_count").alias("total_views"),
            F.sum("add_to_cart_count").alias("total_add_to_cart"),
            F.avg("cart_abandonment_rate").alias("avg_cart_abandonment_rate"),
            F.countDistinct("user_id").alias("unique_users"),
            F.first("purchase_sessions").alias("_purchase_sessions"),
        )
        .withColumn(
            "conversion_rate",
            F.when(F.col("total_views") > 0, F.col("_purchase_sessions") / F.col("total_views")).otherwise(0.0),
        )
        .drop("_purchase_sessions")
        .withColumn("date", F.lit(processing_date))
    )

    out_path = f"{config.delta_warehouse}/product_performance"
    product_perf.write.format("delta").mode("overwrite").option(
        "replaceWhere", f"date = '{processing_date}'"
    ).partitionBy("date").save(out_path)

    count = product_perf.count()
    log.info("product_performance written: %d rows for %s", count, processing_date)

    DeltaTable.forPath(spark, out_path).optimize().executeCompaction()
    DeltaTable.forPath(spark, out_path).vacuum(168)


def build_conversion_funnel(
    spark: SparkSession,
    config: SparkJobConfig,
    user_sessions: DataFrame,
    processing_date: str,
) -> None:
    outcome_counts = (
        user_sessions.groupBy("outcome")
        .agg(F.count("*").alias("session_count"))
    )

    totals = user_sessions.count()

    pivot = (
        outcome_counts.groupBy(F.lit(processing_date).alias("date"))
        .pivot("outcome", ["purchase", "checkout_abandoned", "cart_abandoned", "bounce"])
        .agg(F.first("session_count"))
        .na.fill(0)
    )

    funnel = pivot.withColumn(
        "browse_to_cart_rate",
        F.when(
            F.lit(totals) > 0,
            (F.col("cart_abandoned") + F.col("checkout_abandoned") + F.col("purchase")) / F.lit(totals),
        ).otherwise(0.0),
    ).withColumn(
        "cart_to_checkout_rate",
        F.when(
            (F.col("cart_abandoned") + F.col("checkout_abandoned") + F.col("purchase")) > 0,
            (F.col("checkout_abandoned") + F.col("purchase"))
            / (F.col("cart_abandoned") + F.col("checkout_abandoned") + F.col("purchase")),
        ).otherwise(0.0),
    ).withColumn(
        "checkout_to_purchase_rate",
        F.when(
            (F.col("checkout_abandoned") + F.col("purchase")) > 0,
            F.col("purchase") / (F.col("checkout_abandoned") + F.col("purchase")),
        ).otherwise(0.0),
    ).withColumn(
        "overall_conversion_rate",
        F.when(F.lit(totals) > 0, F.col("purchase") / F.lit(totals)).otherwise(0.0),
    )

    out_path = f"{config.delta_warehouse}/conversion_funnel"
    funnel.write.format("delta").mode("overwrite").option(
        "replaceWhere", f"date = '{processing_date}'"
    ).partitionBy("date").save(out_path)

    count = funnel.count()
    log.info("conversion_funnel written: %d rows for %s", count, processing_date)

    DeltaTable.forPath(spark, out_path).optimize().executeCompaction()
    DeltaTable.forPath(spark, out_path).vacuum(168)


def build_daily_revenue(
    spark: SparkSession,
    config: SparkJobConfig,
    processing_date: str,
) -> None:
    order_events = (
        spark.read.format("iceberg")
        .load("lakehouse.raw.order_events")
        .filter(F.col("event_date") == processing_date)
        .filter(F.col("event_type") == "order_placed")
    )

    revenue_by_payment = (
        order_events.groupBy("payment_method")
        .agg(F.sum("order_total").alias("revenue"))
        .select(
            F.struct(F.col("payment_method"), F.col("revenue")).alias("entry")
        )
    )

    payment_map = revenue_by_payment.agg(
        F.map_from_entries(F.collect_list("entry")).alias("revenue_by_payment_method")
    )

    country_window = Window.orderBy(F.col("country_revenue").desc())
    revenue_by_country = (
        order_events.groupBy("country")
        .agg(F.sum("order_total").alias("country_revenue"))
        .withColumn("rank", F.rank().over(country_window))
        .filter(F.col("rank") <= 10)
        .select(
            F.struct(F.col("country"), F.col("country_revenue")).alias("entry")
        )
        .agg(F.collect_list("entry").alias("revenue_by_country"))
    )

    daily = order_events.agg(
        F.sum("order_total").alias("total_revenue"),
        F.count("order_id").alias("total_orders"),
        F.avg("order_total").alias("avg_order_value"),
        F.sum("discount_amount").alias("total_discount"),
    )

    result = (
        daily.crossJoin(payment_map)
        .crossJoin(revenue_by_country)
        .withColumn("date", F.lit(processing_date))
    )

    out_path = f"{config.delta_warehouse}/daily_revenue"
    result.write.format("delta").mode("overwrite").option(
        "replaceWhere", f"date = '{processing_date}'"
    ).partitionBy("date").save(out_path)

    count = result.count()
    log.info("daily_revenue written: %d rows for %s", count, processing_date)

    DeltaTable.forPath(spark, out_path).optimize().executeCompaction()
    DeltaTable.forPath(spark, out_path).vacuum(168)


def main() -> None:
    processing_date = sys.argv[1] if len(sys.argv) > 1 else (
        date.today() - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    config = SparkJobConfig.from_env()
    config.processing_date = processing_date

    spark = create_spark_session("raw-to-curated", config)

    try:
        windowed_agg = (
            spark.read.format("iceberg")
            .load("lakehouse.raw.windowed_product_aggregations")
            .filter(F.col("event_date") == processing_date)
        )

        user_sessions = (
            spark.read.format("iceberg")
            .load("lakehouse.raw.user_sessions")
            .filter(F.col("session_date") == processing_date)
        )

        jdbc_opts = {
            "url": config.jdbc_url,
            "user": config.postgres_user,
            "password": config.postgres_password,
            "driver": "org.postgresql.Driver",
        }

        products_dim = (
            spark.read.format("jdbc")
            .options(**jdbc_opts)
            .option(
                "dbtable",
                "(SELECT product_id, name, category, subcategory, brand, base_price FROM products) AS p",
            )
            .load()
        )

        users_dim = (
            spark.read.format("jdbc")
            .options(**jdbc_opts)
            .option(
                "dbtable",
                "(SELECT user_id, email, country, acquisition_channel, segment, created_at FROM users) AS u",
            )
            .load()
        )

        categories_dim = (
            spark.read.format("jdbc")
            .options(**jdbc_opts)
            .option(
                "dbtable",
                "(SELECT category_id, name, parent_category FROM categories) AS c",
            )
            .load()
        )

        products_dim.cache()
        users_dim.cache()
        categories_dim.cache()

        build_product_performance(
            spark, config, windowed_agg, user_sessions, products_dim, processing_date
        )
        build_conversion_funnel(spark, config, user_sessions, processing_date)
        build_daily_revenue(spark, config, processing_date)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
