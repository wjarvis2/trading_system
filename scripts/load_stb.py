#!/usr/bin/env python
# --- scripts/load_stb.py ---
import os
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from pathlib import Path
import psycopg2
import traceback
from dotenv import load_dotenv

from src.utils import send_email

# ────────────── Config ────────────── #
load_dotenv()
PG_DSN     = os.getenv("PG_DSN")
RAW_DIR    = Path(__file__).resolve().parent.parent / "data/raw/stb_railcarloads_reports"
TABLE      = "core_energy.fact_series_value"
META       = "core_energy.fact_series_meta"
USER_EMAIL = "jarviswilliamd@gmail.com"
DEBUG      = True

# ────────────── Logging ────────────── #
def log(msg, level="INFO"):
    if DEBUG or level != "DEBUG":
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {level}: {msg}")

def slug(text):
    return str(text).strip().lower().replace(" ", "_").replace(",", "").replace("(", "").replace(")", "")

def extract_metadata(path: Path):
    fname = path.stem
    parts = fname.split("_")
    if len(parts) < 2:
        log(f"⚠️ Unexpected filename format: {path.name}")
        return fname.lower(), datetime.now(timezone.utc)
    railroad = parts[0].lower()
    try:
        obs_date = datetime.strptime(parts[1], "%Y%m%d")
    except:
        log(f"⚠️ Couldn't parse date from {parts[1]}", "WARNING")
        obs_date = datetime.now(timezone.utc)
    return railroad, obs_date

def safe_read_excel(path, **kwargs):
    try:
        return pd.read_excel(path, **kwargs)
    except Exception as e:
        log(f"Error reading Excel: {e}", "ERROR")
        return pd.DataFrame()

def clean_value(value):
    if pd.isna(value): return None
    try:
        val = str(value).replace(",", "").replace(" ", "").strip()
        if val.lower() in {"-", "", "n/a", "na"}: return None
        return float(val)
    except: return None

# ────────────── Table Parsers ────────────── #
# [Same as your original: parse_table_1 through parse_table_6]

# ────────────── Upsert Logic ────────────── #
def upsert_series(cur, series_code: str, obs_date: datetime, value: float):
    log(f"▶️ {series_code} | {obs_date} | {value}")
    cur.execute(f"""
        INSERT INTO {META} (series_code, source_id, description)
        VALUES (%s, (SELECT source_id FROM core_energy.dim_source WHERE name='STB'), %s)
        ON CONFLICT (series_code) DO NOTHING;
    """, (series_code, series_code))

    cur.execute(f"SELECT series_id FROM {META} WHERE series_code=%s", (series_code,))
    sid = cur.fetchone()
    if not sid:
        log(f"Missing series_id for {series_code}", "ERROR")
        return
    series_id = sid[0]

    cur.execute(f"""
        INSERT INTO {TABLE} (series_id, obs_date, value, loaded_at_ts)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (series_id, obs_date, loaded_at_ts) DO NOTHING;
    """, (series_id, obs_date, value, datetime.now(timezone.utc)))

# ────────────── Main ────────────── #
def main():
    log("🚂 STB Loader starting...")

    files = sorted(RAW_DIR.glob("*.xlsx"))
    if not files:
        log("⚠️ No files to process")
        send_email(subject="STB Loader: No files", body="No Excel files found to process.", to=USER_EMAIL)
        return

    all_inserted = []
    failures = []

    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT m.series_code, v.obs_date
            FROM core_energy.fact_series_value v
            JOIN core_energy.fact_series_meta m USING (series_id)
        """)
        existing = set((sc, d.date()) for sc, d in cur.fetchall())

        for f in files:
            railroad, obs_date = extract_metadata(f)
            log(f"📄 Processing {f.name} — Railroad: {railroad}, Date: {obs_date.date()}")

            results = {}
            parsers = [
                ("Speed", parse_table_1),
                ("Dwell", parse_table_2),
                ("CarsOnline", parse_table_3),
                ("Holding", parse_table_5),
                ("Stalled", parse_table_6)
            ]

            try:
                for label, parser in parsers:
                    records = parser(f, railroad, obs_date)
                    new_records = [r for r in records if (r[0], r[1].date()) not in existing]
                    count = 0
                    for sid, ts, val in new_records:
                        try:
                            upsert_series(cur, sid, ts, val)
                            count += 1
                        except Exception as e:
                            log(f"❌ Error inserting {sid}: {e}", "ERROR")
                            conn.rollback()
                            break
                    else:
                        conn.commit()
                    all_inserted.extend(new_records)
                    results[label] = f"{count} new records"

            except Exception as e:
                log(f"❌ Failed to process {f.name}: {e}", "ERROR")
                traceback.print_exc()
                failures.append(f.name)
                conn.rollback()

            log(f"✅ Summary for {f.name}:")
            for label, msg in results.items():
                log(f"  - {label}: {msg}")

    # 📨 Email Logic
    if all_inserted:
        subject = "STB loader: Success"
        body = (
            f"Inserted {len(all_inserted)} new records from {len(files)} files.\n"
            + ("No issues." if not failures else f"Failures: {', '.join(failures)}")
        )
    elif not failures:
        subject = "STB loader: No new data"
        body = f"No new data to insert from {len(files)} files. All values already exist."
    else:
        subject = "STB loader: Partial Failure"
        body = f"Some files failed: {', '.join(failures)}"

    print(body)
    send_email(subject=subject, body=body, to=USER_EMAIL)

if __name__ == "__main__":
    main()
