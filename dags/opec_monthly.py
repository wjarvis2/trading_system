from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 10,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="opec_dag",
    description="Daily OPEC check for MOMR appendix release and ingestion",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0-10 7 * * 1",  # Every Monday 7:00–7:10am ET (covers 2nd Mon release)
    catchup=False,
    tags=["opec", "momr", "fundamentals"],
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
