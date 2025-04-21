#!/usr/bin/env python
"""
Parse latest OPEC MOMR Excel file and load selected tables into core_energy.fact_series_value.
"""

import os
from pathlib import Path
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()
PG_DSN = os.getenv("PG_DSN")
RAW_DIR = Path(__file__).resolve().parent.parent / "data/raw/opec_reports"

# ────────────────────── Table Parsers ───────────────────────── #

def parse_table_11_1(path: Path) -> list[dict]:
    df = pd.read_excel(path, sheet_name="Table 11 - 1", skiprows=5, header=None)
    df = df.dropna(how="all")  # remove blank rows
    df = df[df.columns[1:]]    # drop first column (row labels)
    header_row = 0
    years = df.iloc[header_row].tolist()
    df = df.iloc[1:].copy()
    df.columns = ["region"] + years[1:]

    # fill hierarchical label structure
    df["region"] = df["region"].ffill()
    df = df.melt(id_vars="region", var_name="obs_date", value_name="value")
    df["obs_date"] = pd.to_datetime(df["obs_date"], errors="coerce")
    df = df.dropna(subset=["obs_date", "value"])

    records = []
    for _, row in df.iterrows():
        sid = "opec.table11_1." + row["region"].strip().lower().replace(" ", "_")
        records.append({"series_code": sid, "obs_date": row["obs_date"], "value": row["value"]})
    return records

def parse_table_11_3(path: Path) -> list[dict]:
    df = pd.read_excel(path, sheet_name="Table 11 - 3", skiprows=5, header=None)
    df = df.dropna(how="all")
    df = df[df.columns[1:]]
    header_row = 0
    years = df.iloc[header_row].tolist()
    df = df.iloc[1:].copy()
    df.columns = ["category"] + years[1:]
    df["category"] = df["category"].ffill()
    df = df.melt(id_vars="category", var_name="obs_date", value_name="value")
    df["obs_date"] = pd.to_datetime(df["obs_date"], errors="coerce")
    df = df.dropna(subset=["obs_date", "value"])

    records = []
    for _, row in df.iterrows():
        sid = "opec.table11_3." + row["category"].strip().lower().replace(" ", "_")
        records.append({"series_code": sid, "obs_date": row["obs_date"], "value": row["value"]})
    return records

def parse_table_11_4(path: Path) -> list[dict]:
    df = pd.read_excel(path, sheet_name="Table 11 - 4", skiprows=5, header=None)
    df = df.dropna(how="all")
    df = df[df.columns[1:]]
    header_row = 0
    years = df.iloc[header_row].tolist()
    df = df.iloc[1:].copy()
    df.columns = ["country"] + years[1:]
    df["country"] = df["country"].ffill()
    df = df.melt(id_vars="country", var_name="obs_date", value_name="value")
    df["obs_date"] = pd.to_datetime(df["obs_date"], errors="coerce")
    df = df.dropna(subset=["obs_date", "value"])

    records = []
    for _, row in df.iterrows():
        sid = "opec.table11_4." + row["country"].strip().lower().replace(" ", "_")
        records.append({"series_code": sid, "obs_date": row["obs_date"], "value": row["value"]})
    return records

# ────────────────────── Upsert Logic ───────────────────────── #

def upsert_series(cur, series_code: str, obs_date: datetime, value: float):
    cur.execute("""
        INSERT INTO core_energy.fact_series_meta (series_code, source_id, description)
        VALUES (%s, (SELECT source_id FROM core_energy.dim_source WHERE name='OPEC'), %s)
        ON CONFLICT (series_code) DO NOTHING;
    """, (series_code, series_code))

    cur.execute("SELECT series_id FROM core_energy.fact_series_meta WHERE series_code=%s", (series_code,))
    series_id = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO core_energy.fact_series_value (series_id, obs_date, value, loaded_at_ts)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (series_id, obs_date, loaded_at_ts) DO NOTHING;
    """, (series_id, obs_date, value, datetime.utcnow()))

def main():
    latest = max(RAW_DIR.glob("opec_*.xlsx"))
    print(f"📄 Parsing {latest.name}")

    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        for parser in [parse_table_11_1, parse_table_11_3, parse_table_11_4]:
            records = parser(latest)
            for r in records:
                upsert_series(cur, r["series_code"], r["obs_date"], r["value"])

    print("✓ Loaded OPEC tables 11-1, 11-3, 11-4")

if __name__ == "__main__":
    main()
