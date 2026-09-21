"""Builds per-user RFM features from orders and browsing sessions.

RFM = Recency (how recently did they buy), Frequency (how often) and Monetary
(how much). Each user gets a 1-5 score for each of the three, and the three
scores together decide which segment they land in.

Usage:
    python batch/spark_jobs/user_features.py 2026-09-17
"""

import sys
from datetime import date, timedelta

from pyspark.sql import Window
from pyspark.sql import functions as F

import config
from spark_session import create_spark_session, read_postgres, write_postgres

# Only look at the last 180 days of behaviour.
LOOKBACK_DAYS = 180


def order_features(spark, processing_date):
    """Purchase counts, spend and recency per user."""
    cutoff = F.date_sub(F.lit(processing_date).cast("date"), LOOKBACK_DAYS)

    orders = read_postgres(spark, "orders").filter(
        (F.col("status") != "cancelled")
        & (F.col("created_at") >= cutoff)
        & (F.col("created_at") <= F.lit(processing_date).cast("date"))
    )

    return orders.groupBy("user_id").agg(
        F.count("order_id").alias("order_count"),
        F.sum(F.col("total_amount") - F.col("discount_amount")).alias("total_spend"),
        F.avg(F.col("total_amount") - F.col("discount_amount")).alias("avg_order_value"),
        F.datediff(F.lit(processing_date).cast("date"), F.max("created_at")).alias(
            "days_since_last_order"
        ),
    )


def session_features(spark, processing_date):
    """Browsing behaviour per user, taken from the curated sessions table."""
    cutoff = F.date_sub(F.lit(processing_date).cast("date"), LOOKBACK_DAYS)

    sessions = spark.read.format("delta").load(
        "%s/user_sessions" % config.DELTA_WAREHOUSE
    ).filter(F.col("session_date") >= cutoff)

    return sessions.groupBy("user_id").agg(
        F.count("session_id").alias("session_count"),
        F.avg("duration_seconds").alias("avg_session_duration_seconds"),
        F.avg(F.when(F.col("outcome") == "bounce", 1.0).otherwise(0.0)).alias("bounce_rate"),
        F.datediff(F.lit(processing_date).cast("date"), F.max("session_date")).alias(
            "days_since_last_visit"
        ),
    )


def add_rfm_scores(df):
    """Split users into five equal-sized buckets for each of R, F and M.

    ntile(5) gives 1 to the lowest fifth and 5 to the highest. For recency the
    order is flipped, because a *small* number of days since the last order is
    the good end.
    """
    recency = Window.orderBy(F.col("days_since_last_order").desc())
    frequency = Window.orderBy(F.col("order_count").asc())
    monetary = Window.orderBy(F.col("total_spend").asc())

    scored = (
        df.withColumn("rfm_recency_score", F.ntile(5).over(recency))
        .withColumn("rfm_frequency_score", F.ntile(5).over(frequency))
        .withColumn("rfm_monetary_score", F.ntile(5).over(monetary))
    )

    # Frequency and monetary usually move together, so the segment is decided
    # by recency plus the average of the other two.
    fm = (F.col("rfm_frequency_score") + F.col("rfm_monetary_score")) / 2
    r = F.col("rfm_recency_score")

    segment = (
        F.when((r >= 4) & (fm >= 4), "champions")
        .when((r >= 3) & (fm >= 3), "loyal_customers")
        .when((r >= 4) & (fm < 3), "new_customers")
        .when((r == 3) & (fm < 3), "promising")
        .when((r == 2) & (fm >= 3), "at_risk")
        .when((r <= 2) & (fm >= 4), "cant_lose_them")
        .when((r <= 2) & (fm < 3), "hibernating")
        .otherwise("need_attention")
    )

    return scored.withColumn("rfm_segment", segment)


def main():
    if len(sys.argv) > 1:
        processing_date = sys.argv[1]
    else:
        processing_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    spark = create_spark_session("user-features")
    print("Building user features for %s" % processing_date)

    try:
        users = read_postgres(spark, "users").select(
            "user_id", "email", "country", "acquisition_channel", "segment"
        )
        orders = order_features(spark, processing_date)
        sessions = session_features(spark, processing_date)

        features = (
            users.join(orders, on="user_id", how="left")
            .join(sessions, on="user_id", how="left")
            .fillna({
                "order_count": 0,
                "total_spend": 0.0,
                "avg_order_value": 0.0,
                "session_count": 0,
                "avg_session_duration_seconds": 0.0,
                "bounce_rate": 0.0,
                # 999 means "never" — keeps the column an integer.
                "days_since_last_order": 999,
                "days_since_last_visit": 999,
            })
        )

        features = add_rfm_scores(features)

        # Rough lifetime value: what they spent in the window, projected to a
        # year and nudged up for customers who order often.
        features = features.withColumn(
            "ltv_estimate",
            F.round(
                F.col("total_spend") * (365.0 / LOOKBACK_DAYS) * (1 + F.col("order_count") / 10.0),
                2,
            ),
        ).withColumn("processing_date", F.lit(processing_date).cast("date"))

        path = "%s/user_features" % config.DELTA_WAREHOUSE
        (
            features.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", "processing_date = '%s'" % processing_date)
            .partitionBy("processing_date")
            .save(path)
        )

        # dbt only needs the newest snapshot, so overwrite rather than append.
        write_postgres(features, "analytics.user_features")
        print("Wrote features for %d users" % features.count())

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
