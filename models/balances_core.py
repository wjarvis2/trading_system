#!/usr/bin/env python
"""
Deterministic supply-demand-stocks engine
----------------------------------------

Public API
----------
build_balances(as_of=None) -> pd.DataFrame
    Returns tidy DataFrame with:
      date • Δstocks_US • stocks_US

get_inventory_percentile(bal, window=730) -> pd.Series
    Rolling percentile of stocks / demand for convenience-yield model.
"""

from __future__ import annotations
import pandas as pd
from src.db_utils import load_model_wide

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────
RIG_ELASTICITY_BBL_PER_DAY = 1200   # US crude bbl/d per rig (empirical 2015-24)
RIG_LEAD_WEEKS             = 8      # rigs lead production by ~2 months
WEEKS_PER_MONTH            = 4.345  # convert monthly OPEC to pseudo-weekly

# ──────────────────────────────────────────────────────────────────────────────
# Core
# ──────────────────────────────────────────────────────────────────────────────
def build_balances(as_of: pd.Timestamp | None = None) -> pd.DataFrame:
    """
    Build a weekly US crude balance using the wide view.

    Parameters
    ----------
    as_of : truncate series up to this date (optional, for vintage back-tests)
    """
    w = load_model_wide(as_of)                     # DataFrame from v_model_wide
    w = w.sort_values("date").reset_index(drop=True)

    # --- SUPPLY --------------------------------------------------------------
    shale_growth = (
        w["us_total_rigs"]
        .shift(RIG_LEAD_WEEKS)
        .mul(RIG_ELASTICITY_BBL_PER_DAY)
    )
    supply = (
        w["crude_prod"]
        + w["ngl"].fillna(0)
        + (w["opec_crude_prod"] / WEEKS_PER_MONTH).fillna(0)
        + shale_growth.fillna(0)
    )

    # --- DEMAND --------------------------------------------------------------
    demand = w["gasoline_supplied"] + w["distillate_supplied"]

    # --- NET IMPORTS ---------------------------------------------------------
    net_imports = (
        w["crude_imports"]
        + w["product_imports"]
        - w["crude_exports"]
        - w["product_exports"]
    )

    # --- STOCK CHANGE & LEVEL -----------------------------------------------
    Δstocks = supply - demand + net_imports
    stocks  = w["ending_stocks_us"] + Δstocks.cumsum()

    bal = pd.DataFrame({
        "date": w["date"],
        "Δstocks_US": Δstocks,
        "stocks_US":  stocks,
    })

    return bal.dropna(subset=["Δstocks_US"]).reset_index(drop=True)

# ──────────────────────────────────────────────────────────────────────────────
# Analytics helper
# ──────────────────────────────────────────────────────────────────────────────
def get_inventory_percentile(bal: pd.DataFrame,
                             window: int = 730) -> pd.Series:
    """
    Rolling percentile of inventory over demand (days-of-cover proxy).

    Parameters
    ----------
    bal    : output of build_balances()
    window : look-back in days (default 2 years)

    Returns
    -------
    pd.Series aligned to bal.index
    """
    ratio = bal["stocks_US"] / bal["stocks_US"].rolling(365).mean()
    return ratio.rolling(window, min_periods=90) \
                .apply(lambda x: x.rank(pct=True).iloc[-1], raw=False)
