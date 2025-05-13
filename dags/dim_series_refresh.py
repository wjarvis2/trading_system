# dags/dim_series_refresh.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dim_series_refresh",
    start_date=datetime(2024,1,1),
    schedule_interval=None,   # trigger manually or upstream
    catchup=False,
    tags=["schema"],
) as dag:

    refresh = BashOperator(
        task_id="load_dim_series",
        bash_command="python /app/scripts/load_dim_series_from_csv.py",
        cwd="/app",
    )
