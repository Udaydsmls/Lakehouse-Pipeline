from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import date
from typing import Any

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from batch.spark_jobs.config import SparkJobConfig


def _build_spark(config: SparkJobConfig) -> SparkSession:
    builder = (
        SparkSession.builder.appName("serving-api")
        .master(config.spark_master)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
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
        .config("spark.sql.adaptive.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


_config = SparkJobConfig.from_env()
_spark: SparkSession | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _spark
    _spark = _build_spark(_config)
    app.state.spark = _spark
    app.state.config = _config
    yield
    if _spark:
        _spark.stop()


app = FastAPI(title="LakeHouse Serving API", version="1.0.0", lifespan=lifespan)


class HealthResponse(BaseModel):
    status: str


class ProductPerformanceRow(BaseModel):
    date: str
    product_id: str
    product_name: str | None = None
    category: str | None = None
    subcategory: str | None = None
    brand: str | None = None
    total_views: int
    total_add_to_cart: int
    avg_cart_abandonment_rate: float
    unique_users: int
    conversion_rate: float


class UserFeaturesResponse(BaseModel):
    user_id: str
    email: str | None = None
    country: str | None = None
    acquisition_channel: str | None = None
    segment: str | None = None
    purchase_count_180d: int
    total_spend_180d: float
    avg_order_value_180d: float
    days_since_last_purchase: int
    days_since_last_visit: int
    favourite_category: str
    session_count_180d: int
    avg_session_duration_seconds: float
    bounce_rate: float
    cart_abandonment_rate: float
    ltv_estimate: float
    rfm_recency_score: int
    rfm_frequency_score: int
    rfm_monetary_score: int
    rfm_segment: str


class FunnelRow(BaseModel):
    date: str
    purchase: int
    checkout_abandoned: int
    cart_abandoned: int
    bounce: int
    browse_to_cart_rate: float
    cart_to_checkout_rate: float
    checkout_to_purchase_rate: float
    overall_conversion_rate: float


class SimilarProduct(BaseModel):
    similar_product_id: str
    similarity_score: float


class DailyRevenueRow(BaseModel):
    date: str
    total_revenue: float
    total_orders: int
    avg_order_value: float
    total_discount: float


def _delta_path(name: str) -> str:
    return f"{_config.delta_warehouse}/{name}"


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return HealthResponse(status="ok")


@app.get("/products/{product_id}/performance", response_model=list[ProductPerformanceRow])
async def product_performance(
    product_id: str,
    days: int = Query(default=7, ge=1, le=365),
) -> list[ProductPerformanceRow]:
    spark: SparkSession = app.state.spark
    path = _delta_path("product_performance")

    cutoff = (date.today() - __import__("datetime").timedelta(days=days)).strftime("%Y-%m-%d")

    try:
        df = (
            DeltaTable.forPath(spark, path)
            .toDF()
            .filter(F.col("product_id") == product_id)
            .filter(F.col("date") >= cutoff)
            .orderBy(F.col("date").desc())
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    rows = df.collect()
    if not rows:
        raise HTTPException(status_code=404, detail=f"No performance data for product {product_id!r}")

    return [
        ProductPerformanceRow(
            date=str(r["date"]),
            product_id=r["product_id"],
            product_name=r.get("product_name"),
            category=r.get("category"),
            subcategory=r.get("subcategory"),
            brand=r.get("brand"),
            total_views=int(r["total_views"] or 0),
            total_add_to_cart=int(r["total_add_to_cart"] or 0),
            avg_cart_abandonment_rate=float(r["avg_cart_abandonment_rate"] or 0.0),
            unique_users=int(r["unique_users"] or 0),
            conversion_rate=float(r["conversion_rate"] or 0.0),
        )
        for r in rows
    ]


@app.get("/users/{user_id}/features", response_model=UserFeaturesResponse)
async def user_features(user_id: str) -> UserFeaturesResponse:
    spark: SparkSession = app.state.spark
    path = _delta_path("user_features")

    try:
        df = (
            DeltaTable.forPath(spark, path)
            .toDF()
            .filter(F.col("user_id") == user_id)
            .orderBy(F.col("processing_date").desc())
            .limit(1)
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    rows = df.collect()
    if not rows:
        raise HTTPException(status_code=404, detail=f"User {user_id!r} not found")

    r = rows[0]
    return UserFeaturesResponse(
        user_id=r["user_id"],
        email=r.get("email"),
        country=r.get("country"),
        acquisition_channel=r.get("acquisition_channel"),
        segment=r.get("segment"),
        purchase_count_180d=int(r["purchase_count_180d"] or 0),
        total_spend_180d=float(r["total_spend_180d"] or 0.0),
        avg_order_value_180d=float(r["avg_order_value_180d"] or 0.0),
        days_since_last_purchase=int(r["days_since_last_purchase"] or 999),
        days_since_last_visit=int(r["days_since_last_visit"] or 999),
        favourite_category=r["favourite_category"] or "unknown",
        session_count_180d=int(r["session_count_180d"] or 0),
        avg_session_duration_seconds=float(r["avg_session_duration_seconds"] or 0.0),
        bounce_rate=float(r["bounce_rate"] or 0.0),
        cart_abandonment_rate=float(r["cart_abandonment_rate"] or 0.0),
        ltv_estimate=float(r["ltv_estimate"] or 0.0),
        rfm_recency_score=int(r["rfm_recency_score"] or 1),
        rfm_frequency_score=int(r["rfm_frequency_score"] or 1),
        rfm_monetary_score=int(r["rfm_monetary_score"] or 1),
        rfm_segment=r["rfm_segment"] or "Need Attention",
    )


@app.get("/funnel", response_model=list[FunnelRow])
async def conversion_funnel(
    start_date: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
) -> list[FunnelRow]:
    spark: SparkSession = app.state.spark
    path = _delta_path("conversion_funnel")

    try:
        df = (
            DeltaTable.forPath(spark, path)
            .toDF()
            .filter(F.col("date").between(start_date, end_date))
            .orderBy(F.col("date"))
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    rows = df.collect()
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No funnel data between {start_date} and {end_date}",
        )

    return [
        FunnelRow(
            date=str(r["date"]),
            purchase=int(r.get("purchase") or 0),
            checkout_abandoned=int(r.get("checkout_abandoned") or 0),
            cart_abandoned=int(r.get("cart_abandoned") or 0),
            bounce=int(r.get("bounce") or 0),
            browse_to_cart_rate=float(r["browse_to_cart_rate"] or 0.0),
            cart_to_checkout_rate=float(r["cart_to_checkout_rate"] or 0.0),
            checkout_to_purchase_rate=float(r["checkout_to_purchase_rate"] or 0.0),
            overall_conversion_rate=float(r["overall_conversion_rate"] or 0.0),
        )
        for r in rows
    ]


@app.get("/products/{product_id}/similar", response_model=list[SimilarProduct])
async def similar_products(
    product_id: str,
    limit: int = Query(default=10, ge=1, le=100),
) -> list[SimilarProduct]:
    spark: SparkSession = app.state.spark
    path = _delta_path("product_similarities")

    try:
        df = (
            DeltaTable.forPath(spark, path)
            .toDF()
            .filter(F.col("product_id") == product_id)
            .orderBy(F.col("similarity_score").desc())
            .limit(limit)
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    rows = df.collect()
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No similarity data for product {product_id!r}",
        )

    return [
        SimilarProduct(
            similar_product_id=r["similar_product_id"],
            similarity_score=float(r["similarity_score"]),
        )
        for r in rows
    ]


@app.get("/revenue/daily", response_model=list[DailyRevenueRow])
async def daily_revenue(
    start_date: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
) -> list[DailyRevenueRow]:
    spark: SparkSession = app.state.spark
    path = _delta_path("daily_revenue")

    try:
        df = (
            DeltaTable.forPath(spark, path)
            .toDF()
            .filter(F.col("date").between(start_date, end_date))
            .orderBy(F.col("date"))
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    rows = df.collect()
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No revenue data between {start_date} and {end_date}",
        )

    return [
        DailyRevenueRow(
            date=str(r["date"]),
            total_revenue=float(r["total_revenue"] or 0.0),
            total_orders=int(r["total_orders"] or 0),
            avg_order_value=float(r["avg_order_value"] or 0.0),
            total_discount=float(r["total_discount"] or 0.0),
        )
        for r in rows
    ]
