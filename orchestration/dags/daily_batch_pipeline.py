from __future__ import annotations

import logging
from datetime import datetime, timedelta

import great_expectations as ge
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.sensors.external_task import ExternalTaskSensor

log = logging.getLogger(__name__)

DBT_DIR = "/opt/dbt/lakehouse"
DBT_PROFILES_DIR = "/opt/dbt"
DBT_TARGET = "prod"

SPARK_MASTER = "spark://spark-master:7077"
S3_BUCKET = Variable.get("s3_bucket", default_var="lakehouse-data")
S3_ENDPOINT = Variable.get("s3_endpoint", default_var="https://s3.amazonaws.com")
AWS_ACCESS_KEY = Variable.get("aws_access_key_id", default_var="")
AWS_SECRET_KEY = Variable.get("aws_secret_access_key", default_var="")
ICEBERG_REST_URL = Variable.get("iceberg_rest_url", default_var="http://iceberg-rest:8181")
SLACK_WEBHOOK_CONN = "slack_webhook_default"

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

SNOWFLAKE_ENV = {
    "SNOWFLAKE_ACCOUNT": Variable.get("snowflake_account", default_var=""),
    "SNOWFLAKE_USER": Variable.get("snowflake_user", default_var=""),
    "SNOWFLAKE_PASSWORD": Variable.get("snowflake_password", default_var=""),
    "SNOWFLAKE_ROLE": Variable.get("snowflake_role", default_var="SYSADMIN"),
    "SNOWFLAKE_DATABASE": Variable.get("snowflake_database", default_var="LAKEHOUSE"),
    "SNOWFLAKE_WAREHOUSE": Variable.get("snowflake_warehouse", default_var="COMPUTE_WH"),
}


def slack_failure_alert(context: dict) -> None:
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    log_url = context["task_instance"].log_url
    message = (
        f":red_circle: *DAG failed*\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Task:* `{task_id}`\n"
        f"*Log:* <{log_url}|View logs>"
    )
    hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_WEBHOOK_CONN)
    hook.send(text=message)


def run_ge_checkpoint(checkpoint_name: str) -> None:
    context = ge.get_context()
    result = context.run_checkpoint(checkpoint_name=checkpoint_name)
    if not result.success:
        raise ValueError(
            f"Great Expectations checkpoint '{checkpoint_name}' failed. "
            f"Results: {result}"
        )


def check_raw_data_freshness(**kwargs) -> None:
    import requests

    yesterday = (kwargs["logical_date"] - timedelta(days=1)).strftime("%Y-%m-%d")
    tables_to_check = ["windowed_product_aggregations", "user_sessions"]
    missing = []

    for table in tables_to_check:
        url = f"{ICEBERG_REST_URL}/v1/namespaces/default/tables/{table}/snapshots"
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            snapshots = response.json().get("snapshots", [])
            has_yesterday = any(
                yesterday in str(s.get("summary", {}).get("spark.app.id", ""))
                or yesterday in str(s.get("committed-at", ""))
                for s in snapshots
            )
            if not has_yesterday:
                missing.append(table)
        except Exception as exc:
            log.warning("Failed to query Iceberg metadata for %s: %s", table, exc)
            missing.append(table)

    if missing:
        log.warning(
            "No data found for %s on %s — skipping pipeline run",
            missing,
            yesterday,
        )
        raise AirflowSkipException(
            f"Missing data for {yesterday} in tables: {missing}"
        )


def notify_success(**kwargs) -> None:
    dag_id = kwargs["dag"].dag_id
    execution_date = kwargs["logical_date"].strftime("%Y-%m-%d")
    message = (
        f":large_green_circle: *Daily batch pipeline succeeded*\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Date:* `{execution_date}`"
    )
    hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_WEBHOOK_CONN)
    hook.send(text=message)


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": slack_failure_alert,
}

with DAG(
    dag_id="daily_batch_pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["batch", "production"],
) as dag:

    check_raw_data_freshness_task = PythonOperator(
        task_id="check_raw_data_freshness",
        python_callable=check_raw_data_freshness,
        provide_context=True,
    )

    yesterday = "{{ macros.ds_add(ds, -1) }}"

    raw_to_curated = SparkSubmitOperator(
        task_id="raw_to_curated",
        application="batch/spark_jobs/raw_to_curated.py",
        application_args=[yesterday],
        conf=SPARK_CONF,
        verbose=False,
    )

    user_features = SparkSubmitOperator(
        task_id="user_features",
        application="batch/spark_jobs/user_features.py",
        application_args=[yesterday],
        conf=SPARK_CONF,
        verbose=False,
    )

    product_embeddings_sensor = ExternalTaskSensor(
        task_id="product_embeddings_sensor",
        external_dag_id="weekly_reprocess",
        external_task_id="product_embeddings",
        allowed_states=["success"],
        mode="reschedule",
        timeout=3600,
        poke_interval=300,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "dbt run "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_DIR} "
            f"--target {DBT_TARGET}"
        ),
        env=SNOWFLAKE_ENV,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "dbt test "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_DIR} "
            f"--target {DBT_TARGET}"
        ),
        env=SNOWFLAKE_ENV,
    )

    great_expectations_raw = PythonOperator(
        task_id="great_expectations_raw",
        python_callable=run_ge_checkpoint,
        op_kwargs={"checkpoint_name": "raw_checkpoint"},
    )

    great_expectations_curated = PythonOperator(
        task_id="great_expectations_curated",
        python_callable=run_ge_checkpoint,
        op_kwargs={"checkpoint_name": "curated_checkpoint"},
    )

    great_expectations_warehouse = PythonOperator(
        task_id="great_expectations_warehouse",
        python_callable=run_ge_checkpoint,
        op_kwargs={"checkpoint_name": "warehouse_checkpoint"},
    )

    notify_success_task = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        provide_context=True,
    )

    check_raw_data_freshness_task >> great_expectations_raw >> raw_to_curated
    raw_to_curated >> user_features
    raw_to_curated >> great_expectations_curated
    user_features >> dbt_run
    dbt_run >> dbt_test >> great_expectations_warehouse >> notify_success_task
