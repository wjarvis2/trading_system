from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 10,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="eia_dag",
    description="Weekly download and load of EIA fundamentals",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="30-40 10 * * 3",  # every Wednesday at 10:30–10:40am ET
    catchup=False,
    tags=["eia", "fundamentals"],
) as dag:

    land = BashOperator(
        task_id="download_eia",
        bash_command="PYTHONPATH=/app python /app/src/data_collection/eia_collector.py",
        cwd="/app",
    )

    stage = BashOperator(
        task_id="load_eia_to_db",
        bash_command="PYTHONPATH=/app python /app/scripts/load_eia.py",
        cwd="/app",
    )

    land >> stage
