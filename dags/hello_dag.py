from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="hello_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    say_hi = BashOperator(
        task_id="say_hi",
        bash_command="echo '👋 Hello from Airflow!'"
    )

