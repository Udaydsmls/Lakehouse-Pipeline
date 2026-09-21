"""Daily pipeline: Spark jobs -> dbt -> data quality checks.

Runs once a day for the previous day's data. Each step is a BashOperator so
that the same commands can be run by hand while developing.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/project"
DBT_DIR = "%s/dbt" % PROJECT_DIR

# Airflow's `ds` is the run date, so the data we want is the day before.
PROCESSING_DATE = "{{ macros.ds_add(ds, -1) }}"

default_args = {
    "owner": "uday",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_pipeline",
    description="Curate yesterday's events, build the dbt marts and validate them",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["lakehouse"],
) as dag:

    raw_to_curated = BashOperator(
        task_id="raw_to_curated",
        bash_command=(
            "cd %s && python batch/spark_jobs/raw_to_curated.py %s"
            % (PROJECT_DIR, PROCESSING_DATE)
        ),
    )

    user_features = BashOperator(
        task_id="user_features",
        bash_command=(
            "cd %s && python batch/spark_jobs/user_features.py %s"
            % (PROJECT_DIR, PROCESSING_DATE)
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd %s && dbt run --profiles-dir ." % DBT_DIR,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd %s && dbt test --profiles-dir ." % DBT_DIR,
    )

    data_quality_checks = BashOperator(
        task_id="data_quality_checks",
        bash_command="cd %s && python quality/run_data_checks.py" % PROJECT_DIR,
    )

    raw_to_curated >> user_features >> dbt_run >> dbt_test >> data_quality_checks
