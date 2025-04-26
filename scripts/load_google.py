#!/usr/bin/env python
# --- scripts/load_google.py ---
import os
import pandas as pd
from datetime import datetime
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from src.utils import send_email

# ────────────── Config ────────────── #
load_dotenv()
PG_DSN     = os.getenv("PG_DSN")
RAW_DIR    = Path(__file__).resolve().parent.parent / "data/raw/google_mobility_reports"
TABLE      = "core_energy.fact_series_value"
META       = "core_energy.fact_series_meta"
USER_EMAIL = "jarviswilliamd@gmail.com"

COLUMNS = {
    "retail_and_recreation_percent_change_from_baseline": "retail",
    "grocery_and_pharmacy_percent_change_from_baseline": "grocery",
    "parks_percent_change_from_baseline": "parks",
    "transit_stations_percent_change_from_baseline": "transit",
    "workplaces_percent_change_from_baseline": "work",
    "residential_percent_change_from_baseline": "residential",
}

# ────────────── Canonical Series Mapping ────────────── #
def load_canonical_mappings():
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT series_code FROM core_energy.dim_series")
            allowed_set = set(r[0] for r in cur.fetchall())

            cur.execute("SELECT alias_code, series_code FROM core_energy.dim_series_alias")
            alias_map = {alias: series for alias, series in cur.fetchall()}

    return allowed_set, alias_map

# ────────────── Helpers ────────────── #
def load_existing_obs_dates(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT obs_date FROM core_energy.fact_series_value 
            WHERE series_id IN (
                SELECT series_id FROM core_energy.fact_series_meta 
                WHERE series_code LIKE 'google_mobility.%'
            )
        """)
        return {r[0] for r in cur.fetchall()}

def cache_series_ids(conn):
    cache = {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT series_id, series_code FROM core_energy.fact_series_meta 
            WHERE series_code LIKE 'google_mobility.%'
        """)
        for sid, scode in cur.fetchall():
            cache[scode] = sid
    return cache

def insert_series_meta(cur, series_code):
    cur.execute(f"""
        INSERT INTO {META} (series_code, source_id, description)
        VALUES (%s, (SELECT source_id FROM core_energy.dim_source WHERE name='Google Mobility'), %s)
        ON CONFLICT (series_code) DO NOTHING;
    """, (series_code, series_code))

def parse_google(path: Path, existing_dates: set, allowed_set: set, alias_map: dict) -> list[tuple]:
    df = pd.read_csv(path, parse_dates=["date"])
    df = df[df["country_region_code"] == "US"]
    df = df[df["sub_region_2"].isna()]

    rows = []
    unknown_series = set()

    for _, row in df.iterrows():
        region = ".".join(
            x.strip().lower().replace(" ", "_")
            for x in [row["country_region"], row["sub_region_1"]]
            if pd.notna(x)
        )
        for col, suffix in COLUMNS.items():
            if pd.isna(row[col]):
                continue
            obs_date = row["date"].date()
            if obs_date in existing_dates:
                continue
            sid = f"google_mobility.{region}.{suffix}".lower().strip()
            sid = alias_map.get(sid, sid)

            if sid not in allowed_set:
                unknown_series.add(sid)
                continue

            rows.append((sid, obs_date, row[col]))

    if unknown_series:
        raise ValueError(f"❌ Unapproved or unknown Google Mobility series_code(s): {sorted(unknown_series)}")

    return rows

# ────────────── Main ────────────── #
def main():
    try:
        files = sorted(RAW_DIR.glob("google_mobility_*.csv"))
        if not files:
            print("⚠️ No files found to process.")
            send_email(
                subject="Google Mobility loader: Failed",
                body="No Google Mobility CSV files found in raw data directory.",
                to=USER_EMAIL
            )
            return

        latest = files[-1]
        print(f"📄 Parsing {latest.name}")

        allowed_set, alias_map = load_canonical_mappings()

        with psycopg2.connect(PG_DSN) as conn:
            existing_dates = load_existing_obs_dates(conn)
            cached_ids = cache_series_ids(conn)
            records = parse_google(latest, existing_dates, allowed_set, alias_map)

            print(f"✓ Parsed {len(records):,} new records")

            rows_to_insert = []
            with conn.cursor() as cur:
                for series_code, obs_date, value in records:
                    if series_code not in cached_ids:
                        insert_series_meta(cur, series_code)
                        cur.execute(f"SELECT series_id FROM {META} WHERE series_code=%s", (series_code,))
                        sid = cur.fetchone()
                        if not sid:
                            continue
                        cached_ids[series_code] = sid[0]
                    series_id = cached_ids[series_code]
                    rows_to_insert.append((series_id, obs_date, value, datetime.utcnow()))

                if rows_to_insert:
                    execute_values(cur, f"""
                        INSERT INTO {TABLE} (series_id, obs_date, value, loaded_at_ts)
                        VALUES %s
                        ON CONFLICT (series_id, obs_date) DO NOTHING;
                    """, rows_to_insert)

            conn.commit()

            if rows_to_insert:
                subject = "Google Mobility loader: Success"
                body = (
                    f"Parsed file: {latest.name}\n"
                    f"Inserted {len(rows_to_insert):,} new rows across "
                    f"{len(set(r[0] for r in records)):,} unique series."
                )
                print(body)
                send_email(subject=subject, body=body, to=USER_EMAIL)
            else:
                print(f"No new values inserted from {latest.name}")

    except Exception as e:
        send_email(
            subject="Google Mobility loader: Failed",
            body=f"Error during load: {str(e)}",
            to=USER_EMAIL
        )
        raise

if __name__ == "__main__":
    main()
