#!/usr/bin/env python
"""
US crude balance – “lite” v0.2
-------------------------------------------------
Relies only on columns that are already present in v_model_wide.
Auto-forward fills missing values across sparse reporting periods.

Public API
----------
build_balances(as_of=None) -> pd.DataFrame
get_inventory_percentile(bal, window_days=730) -> pd.Series
"""
from __future__ import annotations
import pandas as pd
import numpy as np
from src.db_utils import load_model_wide

# ───────────────────────── CONSTANTS ─────────────────────────
RIG_ELASTICITY_KBD_PER_RIG = 1.2        # k bbl/d per active rig
RIG_LEAD_WEEKS             = 8          # rigs → production lag
DAYS_PER_WEEK              = 7

# Actual column names in v_model_wide ←–– adjust here if you rename later
COL = {
    "rigs"          : "baker_us_total_rigs",   
    "prod"          : "field_production_of_crude",
    "net_imp"       : "net_imports_of_crude",
    "runs"          : "crude_runs_to_refineries",
    "stocks_level"  : "ending_commercial_crude_stocks",
}

# ───────────────────────── CORE ENGINE ───────────────────────
def build_balances(as_of: pd.Timestamp | None = None) -> pd.DataFrame:
    """
    Weekly crude balance in k bbl.

    Parameters
    ----------
    as_of : pd.Timestamp | None
        Optional vintage cut-off.
    """
    w = load_model_wide(as_of).sort_values("date").reset_index(drop=True)

    # Forward-fill missing values across sparse reporting gaps
    w = w.ffill()

    # Ensure all required cols are present
    missing = [v for v in COL.values() if v not in w.columns]
    if missing:
        raise KeyError(f"v_model_wide is missing columns: {missing}")

    # ----- SUPPLY (k bbl/d) -----
    shale_growth = (
        w[COL["rigs"]]
        .shift(RIG_LEAD_WEEKS)
        .mul(RIG_ELASTICITY_KBD_PER_RIG)
    )

    supply_kbd = w[COL["prod"]] + w[COL["net_imp"]] + shale_growth.fillna(0)

    # ----- DEMAND (k bbl/d) -----
    demand_kbd = w[COL["runs"]]

    # ----- WEEKLY Δ & LEVEL (k bbl) -----
    delta_kbbl_wk = (supply_kbd - demand_kbd) * DAYS_PER_WEEK
    stocks_kbbl   = w[COL["stocks_level"]] + delta_kbbl_wk.cumsum()

    bal = pd.DataFrame({
        "date"           : w["date"],
        "delta_stocks"   : delta_kbbl_wk,
        "stocks_kbbl"    : stocks_kbbl,
    }).dropna(subset=["delta_stocks"]).reset_index(drop=True)

    return bal

# ───────────────────── INVENTORY PERCENTILE ──────────────────
def get_inventory_percentile(
        bal: pd.DataFrame,
        window_days: int = 730
) -> pd.Series:
    """
    Rolling percentile of days-of-cover over the specified window.

    Returns
    -------
    pd.Series (0–1) aligned to bal.index
    """
    # reload wide view to grab matching demand series
    wide = load_model_wide().set_index("date")
    demand_kbd = wide[COL["runs"]].reindex(bal["date"])

    days_cover = bal["stocks_kbbl"] / demand_kbd
    weeks_window = max(4, int(window_days / DAYS_PER_WEEK))

    return (
        days_cover.rolling(weeks_window, min_periods=13)
        .apply(lambda x: np.argsort(np.argsort(x))[-1] / (len(x) - 1), raw=False)
    )
