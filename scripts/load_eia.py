#!/usr/bin/env python
"""
Load all series contained in the latest EIA snapshot CSV
(data/raw/eia_reports/eia_<timestamp>.csv) into Postgres.

Prereqs
-------
* The collector (src/data_collection/eia_collector.py) has already
  written a snapshot CSV.
* .env contains PG_DSN.
"""

import os, json
from pathlib import Path
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()
PG_DSN = os.environ["PG_DSN"]

ROOT_DIR   = Path(__file__).resolve().parent.parent
RAW_DIR    = ROOT_DIR / "data" / "raw" / "eia_reports"
SERIES_CSV = ROOT_DIR / "config" / "eia_series.csv"

# ─────────────────── helpers ──────────────────── #
def latest_snapshot() -> Path:
    """Return Path to newest eia_*.csv file."""
    snaps = sorted(RAW_DIR.glob("eia_*.csv"))
    if not snaps:
        raise FileNotFoundError("No snapshot files in data/raw/eia_reports/")
    return snaps[-1]

def load_series_lookup() -> dict[str, dict]:
    """Return {series_id: {'description': ...}} dict from config CSV."""
    df = pd.read_csv(SERIES_CSV)
    return (
        df.set_index("series_id")["description"]
          .to_dict()
    )

def group_snapshot(path: Path) -> dict[str, pd.DataFrame]:
    """Read snapshot and return {series_id: df}."""
    df = pd.read_csv(path, dtype={"series_id": str})
    # Normalise column names: period OR date -> obs_date
    if "period" in df.columns:
        df.rename(columns={"period": "obs_date"}, inplace=True)
    if "date" in df.columns:
        df.rename(columns={"date": "obs_date"}, inplace=True)
    df["obs_date"] = pd.to_datetime(df["obs_date"], errors="coerce").dt.date
    grouped = {sid: g[["obs_date", "value"]] for sid, g in df.groupby("series_id")}
    return grouped

def upsert_series(cur, series_code: str, descr: str, df: pd.DataFrame):
    # 1) ensure meta row
    cur.execute(
        """
        INSERT INTO core_energy.fact_series_meta (series_code, source_id, description)
        VALUES (%s, (SELECT source_id FROM core_energy.dim_source WHERE name='EIA'), %s)
        ON CONFLICT (series_code) DO NOTHING;
        """,
        (series_code, descr)
    )
    # 2) get FK
    cur.execute(
        "SELECT series_id FROM core_energy.fact_series_meta WHERE series_code=%s",
        (series_code,)
    )
    series_id = cur.fetchone()[0]

    # 3) bulk upsert values
    records = [
        (series_id, r.obs_date, r.value, datetime.utcnow())
        for r in df.itertuples(index=False)
        if pd.notna(r.value) and pd.notna(r.obs_date)
    ]
    execute_values(
        cur,
        """
        INSERT INTO core_energy.fact_series_value
              (series_id, obs_date, value, loaded_at_ts)
        VALUES %s
        ON CONFLICT (series_id, obs_date, loaded_at_ts) DO NOTHING;
        """,
        records
    )

# ─────────────────── main ──────────────────── #
def main():
    snap = latest_snapshot()
    series_lookup = load_series_lookup()
    grouped = group_snapshot(snap)

    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        for sid, df in grouped.items():
            descr = series_lookup.get(sid, sid)   # <- just the string
            upsert_series(cur, sid, descr, df)


    print(f"✓ Loaded {len(grouped)} series from {snap.name}")

if __name__ == "__main__":
    main()
