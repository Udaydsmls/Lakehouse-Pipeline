"""Small FastAPI service over the analytics tables in Postgres.

The Spark jobs write their output to the `analytics` schema and dbt builds the
mart tables on top; this API just exposes a few of those rows over HTTP so the
pipeline output is usable from something other than a dashboard.

Run with:
    uvicorn serving.main:app --reload
Docs are then at http://localhost:8000/docs
"""

import os

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query

load_dotenv()

app = FastAPI(title="Lakehouse Serving API")


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "ecommerce"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
    )


def run_query(sql, params=None):
    """Run a SELECT and return the rows as a list of dicts."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or {})
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/users/{user_id}/features")
def user_features(user_id: str):
    """RFM scores and behaviour features for one user."""
    rows = run_query(
        """
        SELECT *
        FROM analytics.user_features
        WHERE user_id = %(user_id)s
        """,
        {"user_id": user_id},
    )
    if not rows:
        raise HTTPException(status_code=404, detail="User %s not found" % user_id)
    return rows[0]


@app.get("/products/{product_id}/performance")
def product_performance(product_id: str, days: int = Query(7, ge=1, le=365)):
    """Daily views, carts and conversion rate for one product."""
    rows = run_query(
        """
        SELECT *
        FROM analytics.product_performance
        WHERE product_id = %(product_id)s
          AND date >= CURRENT_DATE - %(days)s
        ORDER BY date DESC
        """,
        {"product_id": product_id, "days": days},
    )
    if not rows:
        raise HTTPException(status_code=404, detail="No data for product %s" % product_id)
    return rows


@app.get("/funnel")
def conversion_funnel(days: int = Query(30, ge=1, le=365)):
    """Daily conversion funnel from the dbt mart."""
    return run_query(
        """
        SELECT *
        FROM marts.mart_conversion_funnel
        WHERE date >= CURRENT_DATE - %(days)s
        ORDER BY date DESC
        """,
        {"days": days},
    )


@app.get("/segments")
def user_segments():
    """How many users fall into each RFM segment, and what they are worth."""
    return run_query(
        """
        SELECT
            rfm_segment,
            COUNT(*)              AS user_count,
            ROUND(AVG(ltv_estimate)::numeric, 2) AS avg_ltv
        FROM analytics.user_features
        GROUP BY rfm_segment
        ORDER BY user_count DESC
        """
    )
