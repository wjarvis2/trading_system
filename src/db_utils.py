#!/usr/bin/env python
"""
Light-weight DB helpers
───────────────────────────────────────────────────────────────
get_engine()            → SQLAlchemy engine using PG_* vars in .env
read_sql()              → pandas.read_sql convenience wrapper
load_model_wide()       → SELECT * FROM core_energy.v_model_wide
allowed_series_codes()  → whitelist of series_code pulled live from DB
"""
from __future__ import annotations

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from functools import lru_cache

# ─────────────────── Engine / connection ───────────────────
load_dotenv()  # expects PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PW

def get_engine():
    """
    Build the SQLAlchemy engine from the *single* PG_DSN string.
    Works anywhere (local shell, Airflow container) as long as PG_DSN is set.
    """
    url = os.getenv("PG_DSN")             # e.g. postgresql+psycopg2://user:pw@postgres:5432/energy
    if not url:
        raise RuntimeError("PG_DSN is not defined in the environment")
    return create_engine(url, pool_pre_ping=True)

# ─────────────────── SQL helpers ────────────────────────────
def read_sql(query: str, **kwargs) -> pd.DataFrame:
    """Run *query* with pandas.read_sql against the shared engine."""
    return pd.read_sql(text(query), con=get_engine(), **kwargs)

# ─────────────────── Wide-pivot loader ─────────────────────
# ─────────────────── Wide-pivot loader ─────────────────────
_PIVOT_SQL = "SELECT * FROM core_energy.v_series_wide ORDER BY date"

def load_model_wide(as_of: str | None = None) -> pd.DataFrame:
    """Return the v_series_wide pivot (optionally ≤ *as_of* date)."""
    df = read_sql(_PIVOT_SQL, parse_dates=["date"])
    if as_of is not None:
        df = df[df["date"] <= pd.to_datetime(as_of)]
    return df


# ─────────────────── Whitelist helper ──────────────────────
@lru_cache
def allowed_series_codes() -> set[str]:
    """
    Return a lowercase set of every canonical series_code in the database.
    Cached for the life of the process so multiple loaders stay fast.
    """
    df = read_sql("SELECT series_code FROM core_energy.dim_series")
    return set(df["series_code"].str.lower())
