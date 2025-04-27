# --- dags/refresh_dim_series_mapping_dag.py ---
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os

# ────────────── Load Environment ────────────── #
load_dotenv()
PG_DSN = os.getenv("PG_DSN")
USER_EMAIL = "jarviswilliamd@gmail.com"

# ────────────── Python Task ────────────── #
def refresh_dim_series_mapping():
    engine = create_engine(PG_DSN)
    with engine.begin() as conn:
        conn.execute(text("""
            DELETE FROM core_energy.map_model_series;
            INSERT INTO core_energy.map_model_series (model_col, series_code)
            SELECT model_col, series_code FROM core_energy.dim_series;
        """))

# ────────────── DAG Definition ────────────── #
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": [USER_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="refresh_dim_series_mapping",
    description="Refresh core_energy.map_model_series daily",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 3 * * *",  # Daily at 3:00 AM
    default_args=default_args,
    catchup=False,
    tags=["infra", "warehouse"],
) as dag:

    refresh = PythonOperator(
        task_id="refresh_dim_series_mapping",
        python_callable=refresh_dim_series_mapping
    )
