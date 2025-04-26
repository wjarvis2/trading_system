from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd

# ────────────── Load Environment ────────────── #
load_dotenv()
PG_DSN = os.getenv("PG_DSN")
USER_EMAIL = "jarviswilliamd@gmail.com"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": [USER_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
}

# ────────────── Python Tasks ────────────── #
def check_db_connection():
    engine = create_engine(PG_DSN)
    with engine.begin() as conn:
        df = pd.read_sql("SELECT * FROM core_energy.v_series_wide LIMIT 1", conn)
    assert not df.empty, "❌ v_series_wide is empty or missing!"

def refresh_view():
    engine = create_engine(PG_DSN)
    with engine.begin() as conn:
        conn.execute(text("SELECT core_energy.refresh_v_series_wide();"))
    print("✅ v_series_wide refreshed.")

# ────────────── DAG Definition ────────────── #
with DAG(
    dag_id="refresh_v_series_wide",
    default_args=default_args,
    description="Daily refresh of v_series_wide view after Google loader finishes",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",  # 6 AM daily
    catchup=False,
    tags=["maintenance", "views"],
) as dag:

    wait_for_google_loader = ExternalTaskSensor(
        task_id="wait_for_google_loader",
        external_dag_id="google_mobility_dag",
        external_task_id="load_google_mobility_to_db",
        mode="reschedule",
        timeout=3600,            # increased to 1 hour max wait
        poke_interval=60,       # check every minute
        allowed_states=["success"],
        failed_states=["failed", "skipped"],  # also handle skipped states
    )

    check_db = PythonOperator(
        task_id="check_db_health",
        python_callable=check_db_connection,
    )

    refresh = PythonOperator(
        task_id="refresh_v_series_wide_task",
        python_callable=refresh_view,
    )

    wait_for_google_loader >> check_db >> refresh