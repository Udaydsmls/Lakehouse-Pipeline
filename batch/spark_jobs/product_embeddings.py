from __future__ import annotations

import sys
import logging
from datetime import date, timedelta

import numpy as np
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, FloatType, DoubleType
from pyspark.sql.window import Window

from batch.spark_jobs.config import SparkJobConfig
from batch.spark_jobs.spark_session import create_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def _cosine_similarity_udf():
    @F.udf(returnType=DoubleType())
    def cosine_sim(a: list[float], b: list[float]) -> float:
        if not a or not b:
            return 0.0
        va = np.array(a, dtype=np.float64)
        vb = np.array(b, dtype=np.float64)
        norm_a = np.linalg.norm(va)
        norm_b = np.linalg.norm(vb)
        if norm_a == 0.0 or norm_b == 0.0:
            return 0.0
        return float(np.dot(va, vb) / (norm_a * norm_b))

    return cosine_sim


def main() -> None:
    processing_date = sys.argv[1] if len(sys.argv) > 1 else (
        date.today() - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    config = SparkJobConfig.from_env()
    config.processing_date = processing_date

    cutoff = (date.today() - timedelta(days=90)).strftime("%Y-%m-%d")

    spark = create_spark_session(
        "product-embeddings",
        config,
        extra_configs={"spark.sql.shuffle.partitions": "200"},
    )

    try:
        order_events = (
            spark.read.format("iceberg")
            .load("lakehouse.raw.order_events")
            .filter(F.col("event_type") == "order_placed")
            .filter(F.col("event_date") >= cutoff)
            .select("user_id", "product_id", "order_id")
            .dropDuplicates()
        )

        purchase_counts = (
            order_events.groupBy("user_id", "product_id")
            .agg(F.count("order_id").alias("purchase_count"))
        )

        user_indexer = StringIndexer(inputCol="user_id", outputCol="user_id_indexed")
        product_indexer = StringIndexer(inputCol="product_id", outputCol="product_id_indexed")

        user_model = user_indexer.fit(purchase_counts)
        indexed = user_model.transform(purchase_counts)
        product_model = product_indexer.fit(indexed)
        indexed = product_model.transform(indexed)

        interactions = indexed.select(
            F.col("user_id_indexed").cast("int").alias("user"),
            F.col("product_id_indexed").cast("int").alias("item"),
            F.col("purchase_count").cast("float").alias("rating"),
        )

        als = ALS(
            rank=32,
            maxIter=20,
            regParam=0.1,
            implicitPrefs=True,
            coldStartStrategy="drop",
            userCol="user",
            itemCol="item",
            ratingCol="rating",
            seed=42,
        )

        log.info("Training ALS model on %d interaction rows", interactions.count())
        als_model = als.fit(interactions)

        item_factors: DataFrame = als_model.itemFactors
        item_factors = item_factors.select(
            F.col("id").alias("product_id_indexed"),
            F.col("features").cast(ArrayType(FloatType())).alias("embedding"),
        )

        product_index_map = product_model.labels
        index_to_product = spark.createDataFrame(
            [(float(i), pid) for i, pid in enumerate(product_index_map)],
            schema=["product_id_indexed", "product_id"],
        )

        product_embeddings = item_factors.join(
            index_to_product, on="product_id_indexed", how="inner"
        ).select("product_id", "embedding")

        embeddings_path = f"s3a://{config.s3_bucket}/curated/product_embeddings/"
        product_embeddings.write.mode("overwrite").parquet(embeddings_path)

        log.info(
            "Product embeddings written to %s (%d products)",
            embeddings_path,
            product_embeddings.count(),
        )

        sample_size = min(1000, product_embeddings.count())
        sample_products = product_embeddings.limit(sample_size)
        broadcast_sample = spark.sparkContext.broadcast(
            sample_products.collect()
        )

        cosine_sim = _cosine_similarity_udf()

        similarities_rows: list[tuple[str, str, float]] = []
        for row in broadcast_sample.value:
            pid = row["product_id"]
            emb = row["embedding"]
            scores = [
                (pid, other["product_id"], float(
                    np.dot(np.array(emb, dtype=np.float64), np.array(other["embedding"], dtype=np.float64))
                    / max(
                        np.linalg.norm(np.array(emb, dtype=np.float64))
                        * np.linalg.norm(np.array(other["embedding"], dtype=np.float64)),
                        1e-10,
                    )
                ))
                for other in broadcast_sample.value
                if other["product_id"] != pid
            ]
            scores.sort(key=lambda x: x[2], reverse=True)
            similarities_rows.extend(scores[:10])

        broadcast_sample.unpersist()

        similarities_df = spark.createDataFrame(
            similarities_rows,
            schema=["product_id", "similar_product_id", "similarity_score"],
        )

        similarities_path = f"{config.delta_warehouse}/product_similarities"
        similarities_df.write.format("delta").mode("overwrite").save(similarities_path)

        log.info(
            "Product similarities written to %s (%d rows)",
            similarities_path,
            similarities_df.count(),
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
