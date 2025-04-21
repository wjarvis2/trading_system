from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="google_mobility_dag",
    description="Weekly download and load of Google Mobility data",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 11 * * 3",  # every Wednesday at 11:00am ET
    catchup=False,
    tags=["google", "mobility", "fundamentals"],
) as dag:

    land = BashOperator(
        task_id="download_google_mobility",
        bash_command="PYTHONPATH=/app python /app/src/data_collection/google_mobility_collector.py",
        cwd="/app",
    )

    stage = BashOperator(
        task_id="load_google_mobility_to_db",
        bash_command="PYTHONPATH=/app python /app/scripts/load_google.py",
        cwd="/app",
    )

    land >> stage
