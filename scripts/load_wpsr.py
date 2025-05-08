#!/usr/bin/env python
"""
scripts/load_wpsr.py
────────────────────────────────────────────────────────────────────────────
Load WPSR CSVs → core_energy.fact_series_value

• Parses all *.csv in data/raw/wpsr_reports/
• Extracts current stocks and crude balance values from STUB_1/STUB_2 layout
• Canonicalizes to model_col using dim_series
• Inserts into core_energy.fact_series_value
• Sends email only on success
"""

from __future__ import annotations
import os
from pathlib import Path
from datetime import datetime, UTC
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from src.utils import send_email

# ───────────── Config ─────────────
load_dotenv()
PG_DSN     = os.getenv("PG_DSN")
USER_EMAIL = "jarviswilliamd@gmail.com"
ROOT_DIR   = Path(__file__).resolve().parents[1]
RAW_DIR    = ROOT_DIR / "data/raw/wpsr_reports"
TABLE      = "core_energy.fact_series_value"
META       = "core_energy.fact_series_meta"

SOURCE_NAME = "EIA_WPSR"

# ───────────── Label Map ─────────────
LABELS = {
    # Top of file (stocks)
    "Crude Oil":                          ("wpsr.ending_total_crude",         "ending_total_crude_stocks"),
    "Commercial (Excluding SPR)":        ("wpsr.ending_commercial_crude",    "ending_commercial_crude_stocks"),
    "Strategic Petroleum Reserve (SPR)": ("wpsr.ending_spr_crude",           "ending_spr_crude_stocks"),

    # Crude Oil Supply section
    "Domestic Production":                ("wpsr.domestic_production",         "wpsr.domestic_production"),
    "Alaska":                             ("wpsr.alaska_production",           "wpsr.alaska_production"),
    "Lower 48":                           ("wpsr.lower_48_production",         "wpsr.lower_48_production"),
    "Transfers to Crude Oil Supply":      ("wpsr.transfers_to_supply",         "wpsr.transfers_to_supply"),
    "Net Imports (Including SPR)":        ("wpsr.net_imports",                 "wpsr.net_imports"),
    "Imports":                            ("wpsr.total_imports",               "wpsr.total_imports"),
    "Commercial Crude Oil":               ("wpsr.commercial_imports",          "wpsr.commercial_imports"),
    "Imports by SPR":                     ("wpsr.imports_by_spr",              "wpsr.imports_by_spr"),
    "Imports into SPR by Others":         ("wpsr.imports_into_spr_by_others",  "wpsr.imports_into_spr_by_others"),
    "Exports":                            ("wpsr.exports",                     "wpsr.exports"),
    "Stock Change (+/build; -/draw)":     ("wpsr.stock_change_total",          "wpsr.stock_change_total"),
    "Commercial Stock Change":            ("wpsr.stock_change_commercial",     "wpsr.stock_change_commercial"),
    "SPR Stock Change":                   ("wpsr.stock_change_spr",            "wpsr.stock_change_spr"),
    "Adjustment":                         ("wpsr.adjustment",                  "wpsr.adjustment"),
    "Crude Oil Input to Refineries":      ("wpsr.crude_input_to_refineries",   "wpsr.crude_input_to_refineries"),
}

# ───────────── Extraction ─────────────
def extract_wpsr_file(path: Path) -> list[tuple]:
    df = pd.read_csv(path, header=None)
    try:
        obs_date = pd.to_datetime(df.iloc[0, 1], errors="coerce").date()
    except Exception:
        print(f"⚠️ Could not parse obs_date from {path.name}")
        return []

    records = []
    start_idx = df[(df[0] == "STUB_1") & (df[1] == "STUB_2")].index
    if start_idx.empty:
        print(f"⚠️ Skipping {path.name}: No STUB_1/STUB_2 header found")
        return []

    split_row = start_idx[0]
    top_df = df.iloc[1:split_row]         # pre-STUB block
    crude_df = df.iloc[split_row + 1:]    # post-STUB block

    for label, (series_code, model_col) in LABELS.items():
        # Try top section first
        row = top_df[top_df[0] == label]
        if not row.empty:
            val = pd.to_numeric(row.iloc[0, 1], errors="coerce")
            if pd.notna(val):
                records.append((model_col, series_code, obs_date, val))
                continue

        # Then look in Crude Oil Supply rows
        crude_rows = crude_df[(crude_df[0] == "Crude Oil Supply") & (crude_df[1].str.contains(label, na=False))]
        if not crude_rows.empty:
            val = pd.to_numeric(crude_rows.iloc[0, 2], errors="coerce")
            if pd.notna(val):
                records.append((model_col, series_code, obs_date, val))

    return records

# ───────────── Loader ─────────────
def ensure_meta(cur, series_codes: list[str]):
    cur.execute(
        f"""
        INSERT INTO {META} (series_code, source_id, description)
        SELECT c, (SELECT source_id FROM core_energy.dim_source WHERE name=%s), c
        FROM   (SELECT UNNEST(%s::text[])) AS t(c)
        ON CONFLICT (series_code) DO NOTHING;
        """,
        ("EIA_WPSR", series_codes)
    )

def insert_rows(cur, rows: list[tuple]):
    ensure_meta(cur, [r[1] for r in rows])
    for model_col, series_code, obs_date, value in rows:
        cur.execute(f"SELECT series_id FROM {META} WHERE series_code = %s", (series_code,))
        sid = cur.fetchone()[0]
        execute_values(
            cur,
            f"""
            INSERT INTO {TABLE} (series_id, obs_date, value, loaded_at_ts)
            VALUES %s
            ON CONFLICT (series_id, obs_date) DO NOTHING;
            """,
            [(sid, obs_date, value, datetime.now(UTC))]
        )

# ───────────── Main ─────────────
def main():
    all_files = sorted(RAW_DIR.glob("*.csv"))
    all_rows = []
    for file in all_files:
        print(f"📄 Parsing {file.name}")
        rows = extract_wpsr_file(file)
        if rows:
            all_rows.extend(rows)

    if not all_rows:
        print("⚠️ No WPSR values parsed.")
        return

    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        insert_rows(cur, all_rows)

    print(f"✓ Loaded {len(all_rows)} rows from {len(all_files)} WPSR files → DB")
    send_email(
        subject="WPSR loader: Success",
        body=f"Inserted {len(all_rows)} values from {len(all_files)} WPSR files.",
        to=USER_EMAIL,
    )

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        send_email(subject="WPSR loader: FAILED", body=str(exc), to=USER_EMAIL)
        raise
