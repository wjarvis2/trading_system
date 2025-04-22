"""
Deterministic supply‑demand‑stocks engine
----------------------------------------

Public API
----------
build_balances(ds, as_of) -> pd.DataFrame
    Returns long tidy frame with:
    date • region • product • supply • demand • net_imports • Δstocks • stocks

get_inventory_percentile(bal, window=730) -> pd.Series
    Rolling percentile of stocks / demand for convenience‑yield model.
"""

from __future__ import annotations
import pandas as pd
from pathlib import Path

# ------------------------------------------------------------------ constants
PRODUCTS = ["crude", "gasoline", "distillate"]
REGIONS  = ["US", "OECD_Asia", "OECD_Europe"]

# crude‑per‑rig elasticity bbl/d (calibrated on 2015‑24)
RIG_ELASTICITY = 1200
RIG_LEAD_WEEKS = 8

# ------------------------------------------------------------------ helpers
def _load_pickle(name: str) -> pd.DataFrame:
    """Centralised loader so tests can monkey‑patch."""
    p = Path(f"data/clean/{name}.pkl")
    return pd.read_pickle(p)

# ------------------------------------------------------------------ core
def build_balances(ds: dict[str, pd.DataFrame] | None = None,
                   as_of: pd.Timestamp | None = None) -> pd.DataFrame:
    """
    Merge all upstream sources into one coherent balance table.
    Pass `ds` in tests; in production it falls back to clean pickles.

    Parameters
    ----------
    ds      : dict of already‑cleaned DataFrames (optional)
    as_of   : timestamp to truncate series (for vintage back‑tests)

    """
    if ds is None:
        ds = {
            "eia":   _load_pickle("eia_weekly"),
            "baker": _load_pickle("baker_rigcount"),
            "paj":   _load_pickle("paj_weekly"),
            "stb":   _load_pickle("stb_rail"),
            "google": _load_pickle("google_mobility"),
        }

    if as_of:
        for k in ds:
            ds[k] = ds[k].loc[:as_of]

    # ------------------ SUPPLY ------------------
    sup = (
        ds["eia"]
        .pivot_table(values=["crude_prod", "ngl", "bio"],
                     index="date", columns="region", aggfunc="sum")
        .sum(axis=1, level=0)
        .rename(columns=lambda x: f"supply_{x}")
    )

    # shale growth proxy
    rig_lead = (
        ds["baker"]
        .set_index("date")["us_total_rigs"]
        .shift(RIG_LEAD_WEEKS)
        .mul(RIG_ELASTICITY)
        .to_frame("shale_growth")
    )
    sup["supply_US"] = sup.get("supply_US", 0) + rig_lead["shale_growth"]

    # ------------------ DEMAND ------------------
    dem = (
        ds["eia"]
        .pivot_table(values=["gasoline_supplied", "distillate_supplied"],
                     index="date", columns="region", aggfunc="sum")
        .sum(axis=1, level=0)
        .rename(columns=lambda x: f"demand_{x}")
    )

    # mobility adjustment (elasticity 0.6 per 1 % change)
    mob = (
        ds["google"]
        .set_index("date")["driving_idx"]
        .pct_change()
        .add(1)
        .pow(0.6)
        .rename("mobility_adj")
    )
    dem.update(dem.filter(like="US").mul(mob, axis=0))

    # ------------------ NET IMPORTS ------------------
    netimp = (
        ds["eia"]
        .eval("net_imports = crude_imports + product_imports \
                         - crude_exports - product_exports")
        .set_index("date")["net_imports"]
        .to_frame("net_imports_US")
    )

    # ------------------ COMBINE ------------------
    bal = pd.concat([sup, dem, netimp], axis=1).fillna(0)
    bal["Δstocks_US"] = (
        bal["supply_US"] - bal["demand_US"] + bal["net_imports_US"]
    )
    bal["stocks_US"] = ds["eia"].set_index("date")["ending_stocks_US"] \
                       + bal["Δstocks_US"].cumsum()

    bal = bal.reset_index(names="date")
    return bal

# ------------------------------------------------------------------ analytics
def get_inventory_percentile(bal: pd.DataFrame,
                             window: int = 730) -> pd.Series:
    ratio = bal["stocks_US"] / bal["demand_US"]
    return ratio.rolling(window, min_periods=90) \
                .apply(lambda x: (x.rank(pct=True).iloc[-1]), raw=False)
