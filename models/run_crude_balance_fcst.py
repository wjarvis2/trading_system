"""
run_crude_balance_fcst.py
────────────────────────────────────────────────────────
A minimal baseline forecast function.

• Reads model.crude_balance_driver (history)
• Generates a naïve 26-week forecast
• Bulk-inserts into model.crude_balance_fcast
• Refreshes model.v_crude_balance_full
"""

from __future__ import annotations

import os
from datetime import timedelta

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()
PG_DSN = os.getenv("PG_DSN")
HORIZON_WEEKS = 26


# ────────────────────────────────────────────────────────────────
def simple_baseline(df_hist: pd.DataFrame) -> pd.DataFrame:
    """Ultra-simple forecast: flat production, 4-week average for flows."""
    last_date = df_hist["obs_date"].max()

    future_dates = (
        pd.date_range(
            start=last_date + timedelta(weeks=1),
            periods=HORIZON_WEEKS,
            freq="W-WED",
        ).date
    )

    def last_value(series_code: str) -> float:
        return (
            df_hist.loc[df_hist.series_code == series_code, "value"]
            .dropna()
            .iloc[-1]
        )

    def four_week_mean(series_code: str) -> float:
        return (
            df_hist.loc[df_hist.series_code == series_code, "value"]
            .dropna()
            .tail(4)
            .mean()
        )

    flat_prod = last_value("wpsr.domestic_production")
    mean_imp = four_week_mean("wpsr.net_imports")
    mean_runs = four_week_mean("wpsr.crude_input_to_refineries")
    stk_adj = four_week_mean("wpsr.adjustment")

    weekly_flow = flat_prod + mean_imp - mean_runs + stk_adj
    cumulative = np.cumsum(np.repeat(weekly_flow, HORIZON_WEEKS))

    last_ending = last_value("wpsr.ending_total_crude_stocks")

    return pd.DataFrame(
        {
            "obs_date": future_dates,
            "ending_stocks_forecast": last_ending + cumulative,
        }
    )


# ────────────────────────────────────────────────────────────────
def forecast_python() -> None:
    """Airflow task callable."""
    with psycopg2.connect(PG_DSN) as conn:
        hist = pd.read_sql(
            "SELECT * FROM model.crude_balance_driver ORDER BY obs_date",
            conn,
            parse_dates=["obs_date"],
        )

    fcast = simple_baseline(hist)
    tuples = list(fcast.itertuples(index=False, name=None))

    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        if tuples:
            cur.execute(
                "DELETE FROM model.crude_balance_fcast WHERE obs_date >= %s;",
                (fcast.obs_date.min(),),
            )
            execute_values(
                cur,
                """
                INSERT INTO model.crude_balance_fcast
                (obs_date, ending_stocks_forecast)
                VALUES %s;
                """,
                tuples,
            )
            cur.execute(
                "REFRESH MATERIALIZED VIEW CONCURRENTLY model.v_crude_balance_full;"
            )
            print(f"Inserted {len(tuples)} forecast rows")


# Allow manual CLI run: `python models/run_crude_balance_fcst.py`
if __name__ == "__main__":
    forecast_python()
