#!/usr/bin/env python

import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load .env config
load_dotenv()
PG_DSN = os.getenv("PG_DSN")
assert PG_DSN, "PG_DSN not set in .env"

# Connect and export
engine = create_engine(PG_DSN)
df = pd.read_sql("SELECT * FROM core_energy.dim_series ORDER BY model_col", engine)
df.to_csv("dim_series.csv", index=False)

print("✅ Exported to dim_series.csv")
