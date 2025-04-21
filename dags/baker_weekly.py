from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="baker_dag",
    description="Weekly download and load of Baker Hughes rig count data",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 11 * * 5",  # every Friday at 11:00am ET
    catchup=False,
    tags=["baker", "rig_count", "fundamentals"],
) as dag:

    land = BashOperator(
        task_id="download_baker",
        bash_command="python /app/src/data_collection/baker_collector.py",
        cwd="/app",
    )

    stage = BashOperator(
        task_id="load_baker_to_db",
        bash_command="python /app/scripts/load_baker.py",
        cwd="/app",
    )

    land >> stage
