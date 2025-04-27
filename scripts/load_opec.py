#!/usr/bin/env python
"""
scripts/load_opec.py
────────────────────────────────────────────────────────────────────────────
Parse OPEC MOMR historicals → core_energy.fact_series_value

• Loops over all opec_*.xlsx in data/raw/opec_reports/
• For each file, uses the filename date as obs_date
• Skips files if already loaded into DB
• Dynamically generates _1qcy, _2qcy, etc.
"""

from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime, date
import pandas as pd
import psycopg2
from dotenv import load_dotenv

from src.db_utils import allowed_series_codes
from src.utils    import send_email

# ───────────── Config ─────────────
load_dotenv()
PG_DSN     = os.getenv("PG_DSN")
RAW_DIR    = Path(__file__).resolve().parents[1] / "data/raw/opec_reports"
USER_EMAIL = "jarviswilliamd@gmail.com"

TABLE = "core_energy.fact_series_value"
META  = "core_energy.fact_series_meta"
ALIAS = "core_energy.dim_series_alias"

MIN_DATE = date(1990, 1, 1)

# ───────────── Metadata for 11-1 Mapping ─────────────
FIELDS = {
    11: "oecd_oil_demand",
    21: "non_oecd_oil_demand",
    22: "world_oil_demand",
    30: "china_oil_demand",
    31: "india_oil_demand",
    41: "non_doc_liquids_production",
    42: "doc_ngls_production",
    43: "total_non_doc_plus_ngls",
    45: "opec_crude_production",
    46: "non_opec_doc_crude_production",
    47: "doc_crude_production",
    48: "total_liquids_production",
    49: "balance_stock_change_misc",
    51: "oecd_closing_stocks_commercial",
    52: "oecd_closing_stocks_spr",
    53: "oecd_closing_stocks_total",
    54: "oecd_closing_stocks_oil_on_water",
    56: "days_forward_commercial",
    57: "days_forward_spr",
    58: "days_forward_total",
}

# ───────────── Helpers ─────────────

def suffix(col: int) -> str:
    mapping = {5: "1qcy", 6: "2qcy", 7: "3qcy", 8: "4qcy", 10: "1qny", 11: "2qny", 12: "3qny", 13: "4qny"}
    return mapping.get(col, "")

def normalize_base_field(base_field: str) -> str:
    return base_field.replace("_oil_demand", "_demand")

def upsert_series(cur, sid: str, obs: date, val: float) -> int:
    cur.execute(
        f"""
        INSERT INTO {META} (series_code, source_id, description)
        VALUES (%s, (SELECT source_id FROM core_energy.dim_source WHERE name='OPEC'), %s)
        ON CONFLICT (series_code) DO NOTHING;
        """,
        (sid, sid),
    )
    cur.execute(f"SELECT series_id FROM {META} WHERE series_code=%s", (sid,))
    sid_row = cur.fetchone()
    if not sid_row:
        return 0
    series_id = sid_row[0]
    cur.execute(
        f"""INSERT INTO {TABLE} (series_id, obs_date, value, loaded_at_ts)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (series_id, obs_date) DO NOTHING;""",
        (series_id, obs, val, datetime.utcnow()),
    )
    return cur.rowcount

def extract_file_obs_date(filename: str) -> date:
    try:
        file_date = datetime.strptime(filename.split("_")[1].replace(".xlsx", ""), "%Y-%m").date()
        return file_date
    except Exception as e:
        raise ValueError(f"Could not parse observation date from filename: {filename}") from e

# ───────────── Main ─────────────

def main() -> None:
    try:
        files = sorted(RAW_DIR.glob("opec_*.xlsx"))
        if not files:
            send_email(subject="OPEC loader: Failed", body="No OPEC Excel files found.", to=USER_EMAIL)
            return

        allowed = allowed_series_codes()
        total_new = 0

        with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT alias_code, series_code FROM {ALIAS}")
            alias_map = {a: s for a, s in cur.fetchall()}

            for wb in files:
                print(f"📄 Parsing {wb.name}")
                file_obs_date = extract_file_obs_date(wb.name)

                # Check if already loaded
                cur.execute(
                    f"""
                    SELECT 1
                    FROM {TABLE} fsv
                    JOIN {META} m ON fsv.series_id = m.series_id
                    WHERE fsv.obs_date = %s
                      AND m.source_id = (SELECT source_id FROM core_energy.dim_source WHERE name = 'OPEC')
                    LIMIT 1;
                    """,
                    (file_obs_date,),
                )
                if cur.fetchone():
                    print(f"⚠️  Skipping {wb.name}, already loaded.")
                    continue

                df = pd.read_excel(wb, sheet_name="Table 11 - 1", skiprows=4, header=None)

                for row_idx, base_field in FIELDS.items():
                    if row_idx >= len(df):
                        print(f"⚠️  Row {row_idx} missing from {wb.name}, skipping...")
                        continue

                    row = df.iloc[row_idx]

                    for col_idx in [5, 6, 7, 8, 10, 11, 12, 13]:
                        if col_idx >= len(row):
                            continue

                        val = row[col_idx]
                        if pd.isna(val):
                            continue

                        sfx = suffix(col_idx)
                        full_series_code = f"opec.t11_1.{normalize_base_field(base_field)}_{sfx}"
                        full_series_code = alias_map.get(full_series_code, full_series_code)

                        if full_series_code not in allowed:
                            print(f"⚠️  Skipping unknown OPEC series: {full_series_code}")
                            continue

                        obs_date = file_obs_date
                        total_new += upsert_series(cur, full_series_code, obs_date, val)

        if total_new > 0:
            send_email(subject="OPEC loader: Success",
                       body=f"Inserted {total_new:,} new rows from {len(files)} files.",
                       to=USER_EMAIL)
            print(f"✓ Inserted {total_new:,} new rows")
        else:
            print("No new rows inserted (up-to-date or filtered out)")

    except Exception as e:
        send_email(subject="OPEC loader: Failed", body=f"Error during load: {e}", to=USER_EMAIL)
        raise

if __name__ == "__main__":
    main()
