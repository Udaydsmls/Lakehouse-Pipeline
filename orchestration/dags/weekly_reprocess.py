from __future__ import annotations

import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

log = logging.getLogger(__name__)

SPARK_MASTER = "spark://spark-master:7077"
S3_BUCKET = Variable.get("s3_bucket", default_var="lakehouse-data")
S3_ENDPOINT = Variable.get("s3_endpoint", default_var="https://s3.amazonaws.com")
AWS_ACCESS_KEY = Variable.get("aws_access_key_id", default_var="")
AWS_SECRET_KEY = Variable.get("aws_secret_access_key", default_var="")
ICEBERG_REST_URL = Variable.get("iceberg_rest_url", default_var="http://iceberg-rest:8181")
DELTA_BASE_PATH = f"s3a://{S3_BUCKET}/delta"

DBT_DIR = "/opt/dbt/lakehouse"
DBT_PROFILES_DIR = "/opt/dbt"
DBT_TARGET = "prod"

SNOWFLAKE_ENV = {
    "SNOWFLAKE_ACCOUNT": Variable.get("snowflake_account", default_var=""),
    "SNOWFLAKE_USER": Variable.get("snowflake_user", default_var=""),
    "SNOWFLAKE_PASSWORD": Variable.get("snowflake_password", default_var=""),
    "SNOWFLAKE_ROLE": Variable.get("snowflake_role", default_var="SYSADMIN"),
    "SNOWFLAKE_DATABASE": Variable.get("snowflake_database", default_var="LAKEHOUSE"),
    "SNOWFLAKE_WAREHOUSE": Variable.get("snowflake_warehouse", default_var="COMPUTE_WH"),
}

SPARK_CONF = {
    "spark.master": SPARK_MASTER,
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.endpoint": S3_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": AWS_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": AWS_SECRET_KEY,
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.databricks.delta.retentionDurationCheck.enabled": "false",
}

DELTA_TABLES = [
    "curated/orders",
    "curated/order_items",
    "curated/users",
    "curated/products",
    "curated/sessions",
    "features/user_features",
    "features/product_performance",
]

ICEBERG_TABLES = [
    "windowed_product_aggregations",
    "user_sessions",
]


def backfill_missing_partitions(**kwargs) -> None:
    from airflow.api.client.local_client import Client

    today = kwargs["logical_date"].date()
    lookback_days = 30
    all_dates = [
        (today - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(1, lookback_days + 1)
    ]

    existing_partitions: set[str] = set()
    delta_table = "curated/orders"
    url = (
        f"{ICEBERG_REST_URL}/v1/namespaces/default/tables/{delta_table.replace('/', '_')}"
        f"/partitions"
    )
    try:
        resp = requests.get(url, timeout=30)
        if resp.ok:
            for partition in resp.json().get("partitions", []):
                date_val = partition.get("spec", {}).get("order_date")
                if date_val:
                    existing_partitions.add(date_val)
    except Exception as exc:
        log.warning("Could not fetch partition metadata: %s — backfilling all 30 days", exc)

    missing_dates = [d for d in all_dates if d not in existing_partitions]

    if not missing_dates:
        log.info("No missing partitions found in the last %d days", lookback_days)
        return

    log.info("Triggering backfill for %d missing partitions: %s", len(missing_dates), missing_dates)

    client = Client(None)
    for date_str in missing_dates:
        run_id = f"backfill__{date_str}__{kwargs['run_id']}"
        client.trigger_dag(
            dag_id="daily_batch_pipeline",
            run_id=run_id,
            conf={"backfill_date": date_str},
            execution_date=None,
        )
        log.info("Triggered daily_batch_pipeline for %s (run_id=%s)", date_str, run_id)


def iceberg_maintenance(**kwargs) -> None:
    expiry_cutoff_ms = int(
        (kwargs["logical_date"] - timedelta(days=7)).timestamp() * 1000
    )

    for table_name in ICEBERG_TABLES:
        base_url = f"{ICEBERG_REST_URL}/v1/namespaces/default/tables/{table_name}"

        expire_url = f"{base_url}/snapshots/expire"
        expire_payload = {"older-than-ms": expiry_cutoff_ms}
        try:
            resp = requests.post(expire_url, json=expire_payload, timeout=60)
            resp.raise_for_status()
            log.info(
                "Expired snapshots older than 7 days for table '%s': %s",
                table_name,
                resp.json(),
            )
        except Exception as exc:
            log.error("Failed to expire snapshots for '%s': %s", table_name, exc)
            raise

        rewrite_url = f"{base_url}/rewrite-data-files"
        rewrite_payload = {
            "strategy": "binpack",
            "options": {"target-file-size-bytes": str(128 * 1024 * 1024)},
        }
        try:
            resp = requests.post(rewrite_url, json=rewrite_payload, timeout=300)
            resp.raise_for_status()
            log.info(
                "Triggered data file rewrite (compaction) for table '%s': %s",
                table_name,
                resp.json(),
            )
        except Exception as exc:
            log.error("Failed to compact data files for '%s': %s", table_name, exc)
            raise


DELTA_VACUUM_SCRIPT = """
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("delta_vacuum_weekly").getOrCreate()

tables = """ + str(DELTA_TABLES) + """
base_path = \"""" + DELTA_BASE_PATH + """\"

for table in tables:
    path = f"{base_path}/{table}"
    print(f"Running VACUUM on {path} with 168-hour retention")
    spark.sql(f"VACUUM delta.`{path}` RETAIN 168 HOURS")

spark.stop()
"""

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="weekly_reprocess",
    schedule="0 2 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["weekly", "maintenance"],
) as dag:

    product_embeddings = SparkSubmitOperator(
        task_id="product_embeddings",
        application="batch/spark_jobs/product_embeddings.py",
        conf=SPARK_CONF,
        verbose=False,
    )

    backfill_missing_partitions_task = PythonOperator(
        task_id="backfill_missing_partitions",
        python_callable=backfill_missing_partitions,
        provide_context=True,
    )

    iceberg_maintenance_task = PythonOperator(
        task_id="iceberg_maintenance",
        python_callable=iceberg_maintenance,
        provide_context=True,
    )

    delta_vacuum = BashOperator(
        task_id="delta_vacuum",
        bash_command=(
            "python -c \""
            + DELTA_VACUUM_SCRIPT.replace('"', '\\"').replace("\n", "\\n")
            + "\""
        ),
    )

    dbt_weekly_refresh = BashOperator(
        task_id="dbt_weekly_refresh",
        bash_command=(
            "dbt run "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_DIR} "
            f"--target {DBT_TARGET} "
            "--select marts "
            "--full-refresh"
        ),
        env=SNOWFLAKE_ENV,
    )

    (
        product_embeddings
        >> backfill_missing_partitions_task
        >> iceberg_maintenance_task
        >> delta_vacuum
        >> dbt_weekly_refresh
    )
