#!/usr/bin/env python
"""
scripts/load_google.py
────────────────────────────────────────────────────────────────────────────
Load US-level Google Mobility CSV → core_energy.fact_series_value

• Reads latest google_mobility_*.csv in data/raw/google_mobility_reports/
• Keeps only the US-national aggregates
• Canonicalises series_code via dim_series_alias
• Skips anything not in allowed_series_codes
• Bulk-inserts new obs (FK‑safe)
• Sends success/failure email
"""

from __future__ import annotations

import os, sys
from pathlib import Path
from datetime import datetime, UTC
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from src.db_utils import allowed_series_codes
from src.utils    import send_email

# ───────────── Config ─────────────
load_dotenv()
PG_DSN     = os.getenv("PG_DSN")
USER_EMAIL = "jarviswilliamd@gmail.com"
ROOT_DIR   = Path(__file__).resolve().parents[1]
RAW_DIR    = ROOT_DIR / "data/raw/google_mobility_reports"
ALIAS_TBL  = "core_energy.dim_series_alias"
META       = "core_energy.fact_series_meta"
TABLE      = "core_energy.fact_series_value"

# ───────────── Helpers ─────────────

COLUMNS = {
    "grocery_and_pharmacy_percent_change_from_baseline": "grocery",
    "parks_percent_change_from_baseline": "parks",
    "residential_percent_change_from_baseline": "residential",
    "retail_and_recreation_percent_change_from_baseline": "retail",
    "transit_stations_percent_change_from_baseline": "transit",
    "workplaces_percent_change_from_baseline": "work",
}

def latest_snapshot() -> Path:
    files = sorted(RAW_DIR.glob("google_mobility_*.csv"))
    if not files:
        sys.exit("❌ No Google Mobility CSVs found in raw folder.")
    return files[-1]

def parse_csv(path: Path) -> list[tuple[str, datetime.date, float]]:
    df = pd.read_csv(path, parse_dates=["date"])

    df = df[(df["country_region_code"] == "US") & (df["sub_region_1"].isna())]

    records = []
    for _, row in df.iterrows():
        for col, suffix in COLUMNS.items():
            val = row.get(col)
            if pd.isna(val):
                continue
            series_code = f"google_mobility.united_states.{suffix}"
            records.append((series_code, row["date"].date(), float(val)))
    return records

def fetch_alias_map(cur) -> dict[str, str]:
    cur.execute(f"SELECT alias_code, series_code FROM {ALIAS_TBL}")
    return {a.lower(): s for a, s in cur.fetchall()}

def ensure_meta(cur, series_codes: set[str]):
    cur.execute(
        f"""
        INSERT INTO {META} (series_code, source_id, description)
        SELECT c, (SELECT source_id FROM core_energy.dim_source WHERE name='Google Mobility'), c
        FROM   (SELECT UNNEST(%s::text[])) AS t(c)
        ON CONFLICT (series_code) DO NOTHING;
        """,
        (list(series_codes),),
    )

def insert_records(cur, sid_map: dict[str, int], records: list[tuple[str, datetime.date, float]]):
    payload = [
        (sid_map[series_code], obs_date, value, datetime.now(UTC))
        for series_code, obs_date, value in records
        if series_code in sid_map
    ]
    execute_values(
        cur,
        f"""
        INSERT INTO {TABLE} (series_id, obs_date, value, loaded_at_ts)
        VALUES %s
        ON CONFLICT (series_id, obs_date) DO NOTHING;
        """,
        payload,
    )

# ───────────── Main ─────────────
def main():
    snap = latest_snapshot()
    print(f"📄  Parsing {snap.name}")

    raw_records = parse_csv(snap)
    whitelist   = allowed_series_codes()

    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        alias_map = fetch_alias_map(cur)

        # Canonicalize & whitelist
        canonical_records = []
        for series_code, obs_date, value in raw_records:
            canon = alias_map.get(series_code.lower(), series_code.lower())
            if canon not in whitelist:
                print(f"⚠️  Skipping unapproved Google series_code: {canon}")
                continue
            canonical_records.append((canon, obs_date, value))

        if not canonical_records:
            print("❌ No whitelisted Google series_codes found to insert.")
            return

        series_set = {s for s, _, _ in canonical_records}
        ensure_meta(cur, series_set)

        # Fetch series_id mapping
        cur.execute(f"SELECT series_id, series_code FROM {META}")
        sid_map = {scode: sid for sid, scode in cur.fetchall()}

        insert_records(cur, sid_map, canonical_records)

        print(f"✓ Inserted {len(canonical_records):,} new Google Mobility records → DB")

        send_email(subject="Google Mobility loader: Success",
                   body=f"Snapshot: {snap.name}\nRows: {len(canonical_records):,}",
                   to=USER_EMAIL)

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        send_email(subject="Google Mobility loader: FAILED", body=str(exc), to=USER_EMAIL)
        raise
