#!/usr/bin/env python
# --- dags/refresh_v_series_wide_weekly.py ---

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ────────────── Load Environment ────────────── #
load_dotenv()
PG_DSN = os.getenv("PG_DSN")

default_args = {
    "owner": "airflow",
    "email": ["jarviswilliamd@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,  # Explicitly set
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ────────────── Python Task ────────────── #
def refresh_view():
    engine = create_engine(PG_DSN)
    with engine.begin() as conn:
        conn.execute(text("SELECT core_energy.refresh_v_series_wide();"))
    print("✅ v_series_wide refreshed (weekly).")

# ────────────── DAG Definition ────────────── #
with DAG(
    dag_id="refresh_v_series_wide_weekly",
    default_args=default_args,
    description="Weekly refresh of v_series_wide after all loader DAGs complete",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 7 * * 0",  # Every Sunday at 7:00 AM
    catchup=False,
    tags=["maintenance", "views", "weekly"],
) as dag:

    wait_for_loaders = [
        ExternalTaskSensor(
            task_id=f"wait_for_{dag_id}",
            external_dag_id=dag_id,
            external_task_id=task_id,
            allowed_states=["success", "skipped"],
            failed_states=["failed"],
            mode="reschedule",
            poke_interval=60,
            timeout=3600,  # 1 hour per loader
        )
        for dag_id, task_id in [
            ("baker_dag", "load_baker_to_db"),
            ("eia_dag", "load_eia_to_db"),
            ("opec_dag", "load_opec"),
            ("paj_dag", "load_paj_to_db"),
            ("stb_dag", "load_stb_to_db"),
            ("google_mobility_dag", "load_google_mobility_to_db"),
        ]
    ]

    refresh = PythonOperator(
        task_id="refresh_weekly_model_view",
        python_callable=refresh_view,
    )

    wait_for_loaders >> refresh
