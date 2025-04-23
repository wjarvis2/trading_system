#!/usr/bin/env python
# --- scripts/load_paj.py ---
import os
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from src.utils import send_email

# ────────────── Config ────────────── #
load_dotenv()
PG_DSN     = os.getenv("PG_DSN")
USER_EMAIL = "jarviswilliamd@gmail.com"
RAW_DIR    = Path(__file__).resolve().parent.parent / "data/raw/paj_cruderuns_reports"
TABLE      = "core_energy.fact_series_value"
META       = "core_energy.fact_series_meta"

# ────────────── DB Write ────────────── #
def upsert_series(cur, series_code: str, obs_date: datetime, value: float):
    if pd.isna(obs_date):
        return

    cur.execute(f"""
        INSERT INTO {META} (series_code, source_id, description)
        VALUES (%s, (SELECT source_id FROM core_energy.dim_source WHERE name='PAJ'), %s)
        ON CONFLICT (series_code) DO NOTHING;
    """, (series_code, series_code))

    cur.execute(f"SELECT series_id FROM {META} WHERE series_code = %s", (series_code,))
    sid = cur.fetchone()
    if not sid:
        return
    series_id = sid[0]

    cur.execute(f"""
        INSERT INTO {TABLE} (series_id, obs_date, value, loaded_at_ts)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (series_id, obs_date, loaded_at_ts) DO NOTHING;
    """, (series_id, obs_date, value, datetime.now(timezone.utc)))

# ────────────── Parsers ────────────── #
# [Unchanged] ... parse_crude, parse_products, parse_prices, parse_stockpiles ...

# ────────────── Main Entry ────────────── #
def main():
    try:
        files = sorted(RAW_DIR.glob("*.xls*"))
        if not files:
            send_email(
                subject="PAJ loader: Failed",
                body="No PAJ Excel files found in raw data directory.",
                to=USER_EMAIL
            )
            print("⚠️ No files to process.")
            return

        print(f"📥 Loading {len(files)} PAJ Excel files...")

        all_records = []
        failures = []

        with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT m.series_code, v.obs_date
                FROM core_energy.fact_series_value v
                JOIN core_energy.fact_series_meta m USING (series_id)
            """)
            existing = set(cur.fetchall())

            for f in files:
                print(f"→ {f.name}")
                try:
                    if "crude_sd" in f.name:
                        records = parse_crude(f)
                    elif "product_sd" in f.name:
                        records = parse_products(f)
                    elif "import_price" in f.name:
                        records = parse_prices(f)
                    elif "stockpiling" in f.name:
                        records = parse_stockpiles(f)
                    else:
                        print("  • Skipped (unknown file type)")
                        continue

                    new_records = [
                        r for r in records
                        if (r["series_code"], r["obs_date"]) not in existing
                    ]
                    for r in new_records:
                        upsert_series(cur, r["series_code"], r["obs_date"], r["value"])
                    all_records.extend(new_records)
                    print(f"  ✓ Inserted {len(new_records)} new records")

                except Exception as e:
                    print(f"  ⚠️ Failed to load {f.name}: {e}")
                    failures.append(f.name)

        # 📬 Email Summary
        if all_records:
            subject = "PAJ loader: Success"
            body = (
                f"✓ Loaded {len(all_records)} new records from {len(files)} files.\n"
                + ("No errors." if not failures else f"Failures: {', '.join(failures)}")
            )
            send_email(subject=subject, body=body, to=USER_EMAIL)
        elif failures:
            subject = "PAJ loader: Partial Failure"
            body = (
                f"Attempted to load {len(files)} files.\n"
                f"Failures: {', '.join(failures)}"
            )
            send_email(subject=subject, body=body, to=USER_EMAIL)
        else:
            print("No new data to insert. All values already existed.")

    except Exception as e:
        send_email(subject="PAJ loader: Failed", body=str(e), to=USER_EMAIL)
        raise

if __name__ == "__main__":
    main()
