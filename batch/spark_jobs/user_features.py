from __future__ import annotations

import sys
import logging
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pads
import ray
from pyspark.sql import functions as F

from batch.spark_jobs.config import SparkJobConfig
from batch.spark_jobs.spark_session import create_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

RFM_SEGMENTS: dict[str, str] = {
    "555": "Champion",
    "554": "Champion",
    "544": "Champion",
    "545": "Champion",
    "454": "Champion",
    "455": "Champion",
    "445": "Champion",
    "543": "Loyal Customer",
    "444": "Loyal Customer",
    "435": "Loyal Customer",
    "355": "Loyal Customer",
    "354": "Loyal Customer",
    "345": "Loyal Customer",
    "344": "Loyal Customer",
    "335": "Loyal Customer",
    "512": "Potential Loyalist",
    "511": "Potential Loyalist",
    "422": "Potential Loyalist",
    "421": "Potential Loyalist",
    "412": "Potential Loyalist",
    "411": "Potential Loyalist",
    "311": "Potential Loyalist",
    "525": "Potential Loyalist",
    "524": "Potential Loyalist",
    "523": "Potential Loyalist",
    "522": "Potential Loyalist",
    "521": "Potential Loyalist",
    "515": "Potential Loyalist",
    "514": "Potential Loyalist",
    "513": "Potential Loyalist",
    "425": "Potential Loyalist",
    "424": "Potential Loyalist",
    "413": "Potential Loyalist",
    "331": "Promising",
    "321": "Promising",
    "312": "Promising",
    "221": "Promising",
    "213": "Promising",
    "155": "Cannot Lose Them",
    "154": "Cannot Lose Them",
    "144": "Cannot Lose Them",
    "214": "Cannot Lose Them",
    "215": "Cannot Lose Them",
    "115": "Cannot Lose Them",
    "114": "Cannot Lose Them",
    "113": "Cannot Lose Them",
    "255": "At Risk",
    "254": "At Risk",
    "245": "At Risk",
    "244": "At Risk",
    "253": "At Risk",
    "252": "At Risk",
    "243": "At Risk",
    "242": "At Risk",
    "235": "At Risk",
    "234": "At Risk",
    "225": "At Risk",
    "224": "At Risk",
    "111": "Lost",
    "112": "Lost",
    "121": "Lost",
    "131": "Lost",
    "141": "Lost",
    "151": "Lost",
}


def _rfm_segment(r: int, f: int, m: int) -> str:
    key = f"{r}{f}{m}"
    return RFM_SEGMENTS.get(key, "Need Attention")


def _quintile_score(series: pd.Series, ascending: bool = True) -> pd.Series:
    labels = [1, 2, 3, 4, 5] if ascending else [5, 4, 3, 2, 1]
    try:
        return pd.qcut(series, q=5, labels=labels, duplicates="drop").astype(int)
    except ValueError:
        return pd.Series(3, index=series.index)


@ray.remote
def compute_user_features_partition(
    session_records: list[dict[str, Any]],
    reference_date: str,
) -> pd.DataFrame:
    if not session_records:
        return pd.DataFrame()

    ref = pd.Timestamp(reference_date)
    sessions = pd.DataFrame(session_records)

    sessions["session_date"] = pd.to_datetime(sessions["session_date"], errors="coerce")
    sessions["last_event_ts"] = pd.to_datetime(sessions.get("last_event_ts", sessions["session_date"]), errors="coerce")
    sessions["duration_seconds"] = pd.to_numeric(sessions.get("duration_seconds", 0), errors="coerce").fillna(0)
    sessions["revenue"] = pd.to_numeric(sessions.get("revenue", 0), errors="coerce").fillna(0)

    result_rows: list[dict[str, Any]] = []

    for user_id, grp in sessions.groupby("user_id"):
        purchase_rows = grp[grp["outcome"] == "purchase"]
        bounce_rows = grp[grp["outcome"] == "bounce"]
        cart_abandoned_rows = grp[grp["outcome"] == "cart_abandoned"]

        purchase_count = len(purchase_rows)
        total_spend = float(purchase_rows["revenue"].sum())
        avg_order_value = float(purchase_rows["revenue"].mean()) if purchase_count > 0 else 0.0

        if not purchase_rows.empty:
            last_purchase_date = purchase_rows["session_date"].max()
            days_since_last_purchase = max(0, (ref - last_purchase_date).days)
        else:
            days_since_last_purchase = 999

        if not grp.empty:
            last_visit_date = grp["session_date"].max()
            days_since_last_visit = max(0, (ref - last_visit_date).days)
        else:
            days_since_last_visit = 999

        if "category" in grp.columns and not purchase_rows.empty:
            fav_category = (
                purchase_rows["category"].value_counts().index[0]
                if not purchase_rows["category"].dropna().empty
                else "unknown"
            )
        else:
            fav_category = "unknown"

        session_count = len(grp)
        avg_session_duration = float(grp["duration_seconds"].mean()) if session_count > 0 else 0.0
        bounce_rate = float(len(bounce_rows) / session_count) if session_count > 0 else 0.0
        cart_sessions = len(cart_abandoned_rows) + len(purchase_rows)
        cart_abandonment_rate = (
            float(len(cart_abandoned_rows) / cart_sessions) if cart_sessions > 0 else 0.0
        )

        ltv_estimate = total_spend * (365.0 / 180.0) * (1.0 + purchase_count / 10.0)

        result_rows.append(
            {
                "user_id": user_id,
                "purchase_count_180d": purchase_count,
                "total_spend_180d": total_spend,
                "avg_order_value_180d": avg_order_value,
                "days_since_last_purchase": days_since_last_purchase,
                "days_since_last_visit": days_since_last_visit,
                "favourite_category": fav_category,
                "session_count_180d": session_count,
                "avg_session_duration_seconds": avg_session_duration,
                "bounce_rate": bounce_rate,
                "cart_abandonment_rate": cart_abandonment_rate,
                "ltv_estimate": ltv_estimate,
            }
        )

    return pd.DataFrame(result_rows)


def _read_parquet_streaming(s3_path: str, columns: list[str]) -> pd.DataFrame:
    ds = pads.dataset(s3_path, format="parquet")
    scanner = ds.scanner(columns=columns, batch_size=65536)
    batches: list[pd.DataFrame] = []
    for batch in scanner.to_batches():
        batches.append(batch.to_pandas())
    return pd.concat(batches, ignore_index=True) if batches else pd.DataFrame(columns=columns)


def _add_rfm_scores(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["rfm_recency_score"] = _quintile_score(df["days_since_last_purchase"], ascending=False)
    df["rfm_frequency_score"] = _quintile_score(df["purchase_count_180d"], ascending=True)
    df["rfm_monetary_score"] = _quintile_score(df["total_spend_180d"], ascending=True)
    df["rfm_segment"] = df.apply(
        lambda row: _rfm_segment(
            int(row["rfm_recency_score"]),
            int(row["rfm_frequency_score"]),
            int(row["rfm_monetary_score"]),
        ),
        axis=1,
    )
    return df


def main() -> None:
    processing_date = sys.argv[1] if len(sys.argv) > 1 else (
        date.today() - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    config = SparkJobConfig.from_env()
    config.processing_date = processing_date

    cutoff_date = (
        pd.Timestamp(processing_date) - pd.Timedelta(days=180)
    ).strftime("%Y-%m-%d")

    spark = create_spark_session("user-features", config)

    try:
        session_columns = [
            "user_id",
            "session_date",
            "last_event_ts",
            "outcome",
            "duration_seconds",
            "revenue",
            "category",
        ]

        sessions_path = f"s3a://{config.s3_bucket}/curated/user_sessions/"
        log.info("Reading user_sessions from %s", sessions_path)
        sessions_pd = _read_parquet_streaming(sessions_path, session_columns)
        sessions_pd["session_date"] = pd.to_datetime(sessions_pd["session_date"], errors="coerce")
        sessions_pd = sessions_pd[sessions_pd["session_date"] >= cutoff_date]

        if sessions_pd.empty:
            log.warning("No session data found for the 180-day window ending %s", processing_date)
            spark.stop()
            return

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)

        partition_size = max(1, len(sessions_pd) // 64)
        user_ids = sessions_pd["user_id"].unique()
        user_partitions: list[list[str]] = [
            user_ids[i : i + partition_size].tolist()
            for i in range(0, len(user_ids), partition_size)
        ]

        records = sessions_pd.to_dict("records")
        user_to_records: dict[str, list[dict[str, Any]]] = {}
        for record in records:
            uid = record["user_id"]
            user_to_records.setdefault(uid, []).append(record)

        futures = [
            compute_user_features_partition.remote(
                [r for uid in partition for r in user_to_records.get(uid, [])],
                processing_date,
            )
            for partition in user_partitions
        ]

        result_frames = ray.get(futures)
        features_pd = pd.concat([f for f in result_frames if not f.empty], ignore_index=True)
        features_pd = _add_rfm_scores(features_pd)

        log.info("Computed features for %d users", len(features_pd))

        jdbc_opts = {
            "url": config.jdbc_url,
            "user": config.postgres_user,
            "password": config.postgres_password,
            "driver": "org.postgresql.Driver",
        }

        users_dim = (
            spark.read.format("jdbc")
            .options(**jdbc_opts)
            .option(
                "dbtable",
                "(SELECT user_id, email, country, acquisition_channel, segment, created_at FROM users) AS u",
            )
            .load()
        )

        features_spark = spark.createDataFrame(features_pd)

        final_df = (
            features_spark.join(users_dim, on="user_id", how="left")
            .withColumn("processing_date", F.lit(processing_date))
        )

        out_path = f"{config.delta_warehouse}/user_features"
        final_df.write.format("delta").mode("overwrite").option(
            "replaceWhere", f"processing_date = '{processing_date}'"
        ).partitionBy("processing_date").save(out_path)

        log.info("user_features written: %d rows for %s", final_df.count(), processing_date)

    finally:
        if ray.is_initialized():
            ray.shutdown()
        spark.stop()


if __name__ == "__main__":
    main()
