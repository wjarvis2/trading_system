from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 10,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="paj_dag",
    description="Frequent PAJ ingestion of crude and product statistics",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 6,14,22 * * *",  # 3x daily (6am, 2pm, 10pm ET)
    catchup=False,
    tags=["paj", "japan", "refining", "fundamentals"],
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
