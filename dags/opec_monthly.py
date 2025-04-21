from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="opec_monthly",
    description="Daily OPEC check for MOMR release and ingestion",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 7 * * *",  # 7:00 AM ET daily (12:00 GMT)
    catchup=False,
    tags=["opec", "momr"],
) as dag:

    land = BashOperator(
        task_id="download_opec",
        bash_command="PYTHONPATH=/app python /app/src/data_collection/opec_collector.py",
        cwd="/app",
    )

    stage = BashOperator(
        task_id="load_opec",
        bash_command="PYTHONPATH=/app python /app/scripts/load_opec.py",
        cwd="/app",
    )

    land >> stage
