from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="paj_dag",
    description="Weekly download and load of PAJ crude and product stats",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 11 * * 3",  # every Wednesday at 11:00am ET
    catchup=False,
    tags=["paj", "japan", "fundamentals"],
) as dag:

    land = BashOperator(
        task_id="download_paj",
        bash_command="PYTHONPATH=/app python /app/src/data_collection/paj_collector.py",
        cwd="/app",
    )

    stage = BashOperator(
        task_id="load_paj_to_db",
        bash_command="PYTHONPATH=/app python /app/scripts/load_paj.py",
        cwd="/app",
    )

    land >> stage
