#!/usr/bin/env python

"""
Load curated dim_series metadata into core_energy.dim_series table.
Supports UPSERT to update existing model_col records.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load DB credentials
load_dotenv()
PG_DSN = os.getenv("PG_DSN")
assert PG_DSN, "Missing PG_DSN in .env"

# Config
CSV_PATH = "config/dim_series.csv"


# Read and validate CSV
df = pd.read_csv(CSV_PATH)
required_cols = {"model_col", "series_code", "unit", "freq", "description", "tags"}
assert required_cols.issubset(df.columns), f"Missing columns in CSV. Required: {required_cols}"

# Format tags as Postgres array literals
def format_tags(val):
    if pd.isna(val) or str(val).strip() == "":
        return "{}"
    val = str(val).strip()
    return val if val.startswith("{") else "{" + val + "}"

df["tags"] = df["tags"].apply(format_tags)

# Write to database
engine = create_engine(PG_DSN)
with engine.begin() as conn:
    for _, row in df.iterrows():
        conn.execute(text("""
            INSERT INTO core_energy.dim_series (model_col, series_code, unit, freq, description, tags)
            VALUES (:model_col, :series_code, :unit, :freq, :description, :tags)
            ON CONFLICT (model_col) DO UPDATE
            SET series_code = EXCLUDED.series_code,
                unit        = EXCLUDED.unit,
                freq        = EXCLUDED.freq,
                description = EXCLUDED.description,
                tags        = EXCLUDED.tags
        """), row.to_dict())

print("✅ core_energy.dim_series successfully updated.")
