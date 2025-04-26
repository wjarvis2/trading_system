#!/usr/bin/env python
"""
Light-weight DB helpers
-----------------------

get_engine()      → SQLAlchemy engine using PG_* vars in .env
read_sql(query)   → pd.read_sql wrapper
load_model_wide() → SELECT * FROM core_energy.v_model_wide
"""

import os, pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()   # expects PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PW in .env

# -------------------------------------------------------------------------
def get_engine():
    url = (
        f"postgresql+psycopg2://{os.getenv('PG_USER')}:{os.getenv('PG_PW')}"
        f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
    )
    return create_engine(url, pool_pre_ping=True)

def read_sql(query: str, **kwargs) -> pd.DataFrame:
    return pd.read_sql(text(query), con=get_engine(), **kwargs)

# -------------------------------------------------------------------------
PIVOT_SQL = "SELECT * FROM core_energy.v_model_wide ORDER BY date"

def load_model_wide(as_of=None) -> pd.DataFrame:
    df = read_sql(PIVOT_SQL, parse_dates=["date"])
    if as_of is not None:
        df = df[df["date"] <= pd.to_datetime(as_of)]
    return df
