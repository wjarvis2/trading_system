"""
Crude-Balance Forecast DAG
────────────────────────────────────────────────────────────
Workflow
  1. Pull last week’s realised WPSR drivers from v_model_wide and
     insert them into model.crude_balance_driver (COALESCE to zero
     for columns that didn’t exist pre-2022, e.g. transfers_to_supply).
  2. Run the Python forecast function → populate
     model.crude_balance_fcast.
  3. (Inside the Python task) refresh model.v_crude_balance_full.

Prerequisites
  • Airflow env-var connection  AIRFLOW_CONN_TRADING_DB  must be set
    in docker-compose (points to Postgres).  The DAG references it via
    postgres_conn_id="trading_db".

Tags: balance, wpsr
"""

from datetime import timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from models.run_crude_balance_fcst import forecast_python


# ────────────────────────────────────────────────────────────────
default_args = {
    "owner": "energy",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crude_balance_weekly",
    description="Forecast U.S. crude balance weekly using core identity drivers",
    start_date=days_ago(1),               # first run = now; no backfill
    schedule_interval="0 8 * * THU",      # every Thu 08:00 ET
    catchup=False,
    default_args=default_args,
    tags=["balance", "wpsr"],
) as dag:

    # ── 1. Insert last week's realised drivers ──────────────────
    insert_hist_drivers = PostgresOperator(
        task_id="insert_hist_drivers",
        postgres_conn_id="trading_db",
        sql="""
WITH latest AS (
    SELECT *
    FROM core_energy.v_model_wide
    ORDER BY date DESC
    LIMIT 1
)
INSERT INTO model.crude_balance_driver (obs_date, series_code, value)
SELECT
    date AS obs_date,
    UNNEST(ARRAY[
        'wpsr.domestic_production',
        'wpsr.alaska_production',
        'wpsr.lower_48_production',
        'wpsr.transfers_to_supply',
        'wpsr.net_imports',
        'wpsr.total_imports',
        'wpsr.commercial_imports',
        'wpsr.imports_by_spr',
        'wpsr.imports_into_spr_by_others',
        'wpsr.exports',
        'wpsr.stock_change_total',
        'wpsr.stock_change_commercial',
        'wpsr.stock_change_spr',
        'wpsr.adjustment',
        'wpsr.crude_input_to_refineries'
    ]) AS series_code,
    UNNEST(ARRAY[
        COALESCE(wpsr_domestic_production,        0),
        COALESCE(wpsr_alaska_production,          0),
        COALESCE(wpsr_lower_48_production,        0),
        COALESCE(wpsr_transfers_to_supply,        0),
        COALESCE(wpsr_net_imports,                0),
        COALESCE(wpsr_total_imports,              0),
        COALESCE(wpsr_commercial_imports,         0),
        COALESCE(wpsr_imports_by_spr,             0),
        COALESCE(wpsr_imports_into_spr_by_others, 0),
        COALESCE(wpsr_exports,                    0),
        COALESCE(wpsr_stock_change_total,         0),
        COALESCE(wpsr_stock_change_commercial,    0),
        COALESCE(wpsr_stock_change_spr,           0),
        COALESCE(wpsr_adjustment,                 0),
        COALESCE(wpsr_crude_input_to_refineries,  0)
    ]) AS value
FROM latest
ON CONFLICT DO NOTHING;
""",
    )

    # ── 2. Run forecast (includes view refresh) ─────────────────
    run_forecast = PythonOperator(
        task_id="run_balance_forecast",
        python_callable=forecast_python,   # uses PG_DSN env var internally
    )

    insert_hist_drivers >> run_forecast
