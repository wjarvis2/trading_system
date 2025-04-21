from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="eurostat_daily",
    description="Daily Eurostat fundamentals ingestion from .tsv.gz files",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 7 * * *",  # 7:00 AM Eastern / 12:00 CET
    catchup=False,
    tags=["eurostat", "fundamentals"],
) as dag:

    land = BashOperator(
        task_id="download_eurostat",
        bash_command="PYTHONPATH=/app python /app/src/data_collection/eurostat_collector.py",
        cwd="/app",
    )

    stage = BashOperator(
        task_id="load_eurostat",
        bash_command="PYTHONPATH=/app python /app/scripts/load_eurostat.py",
        cwd="/app",
    )

    land >> stage
