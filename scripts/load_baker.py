#!/usr/bin/env python
# --- scripts/load_baker.py ---
import os
import pandas as pd
from datetime import datetime
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

from src.utils import send_email

# ────────────── Config ────────────── #
load_dotenv()
PG_DSN     = os.getenv("PG_DSN")
RAW_DIR    = Path(__file__).resolve().parent.parent / "data/raw/bh_rigcount_reports"
TABLE      = "core_energy.fact_series_value"
META       = "core_energy.fact_series_meta"
USER_EMAIL = "jarviswilliamd@gmail.com"

# ────────────── Parser ────────────── #
def parse_baker(path: Path) -> pd.DataFrame:
    df = pd.read_excel(
        path,
        sheet_name="NAM Weekly",
        skiprows=10,
        usecols="A:L",
    )
    df = df.dropna(subset=["US_PublishDate", "Rig Count Value"])
    df = df.rename(columns={"Rig Count Value": "value"})

    df["obs_date"] = pd.to_datetime(df["US_PublishDate"], errors="coerce").dt.date
    df["series_code"] = (
        "baker." + df["Country"].str.lower()
        + "." + df["DrillFor"].str.lower()
        + "." + df["Trajectory"].str.lower()
        + "." + df["Basin"].str.lower().str.replace(" ", "_")
    )
    return df[["series_code", "obs_date", "value"]].dropna()

# ────────────── Upsert Logic ────────────── #
def upsert_series(cur, series_code: str, obs_date: datetime, value: float):
    cur.execute(f"""
        INSERT INTO {META} (series_code, source_id, description)
        VALUES (%s, (SELECT source_id FROM core_energy.dim_source WHERE name='Baker Hughes'), %s)
        ON CONFLICT (series_code) DO NOTHING;
    """, (series_code, series_code))

    cur.execute(f"SELECT series_id FROM {META} WHERE series_code = %s", (series_code,))
    series_id = cur.fetchone()[0]

    cur.execute(f"""
        INSERT INTO {TABLE} (series_id, obs_date, value, loaded_at_ts)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (series_id, obs_date, loaded_at_ts) DO NOTHING;
    """, (series_id, obs_date, value, datetime.utcnow()))

# ────────────── Main ────────────── #
def main():
    try:
        latest = max(RAW_DIR.glob("bh_rigcount_*.xlsx"))
        print(f"📄 Parsing {latest.name}")
        df = parse_baker(latest)

        with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
            # Fetch all known Baker series
            cur.execute(f"""
                SELECT series_code, series_id FROM {META}
                WHERE series_code LIKE 'baker.%'
            """)
            known_series = dict(cur.fetchall())

            # Avoid empty IN clause if no series exist
            existing = set()
            if known_series:
                cur.execute(f"""
                    SELECT series_id, obs_date FROM {TABLE}
                    WHERE series_id IN %s
                """, (tuple(known_series.values()),))
                existing = {(r[0], r[1]) for r in cur.fetchall()}

            insert_count = 0
            series_seen = set()

            for _, row in df.iterrows():
                sid = row["series_code"]
                obs = row["obs_date"]
                val = row["value"]

                if sid not in known_series:
                    upsert_series(cur, sid, obs, val)
                    insert_count += 1
                    series_seen.add(sid)
                    continue

                if (known_series[sid], obs) not in existing:
                    upsert_series(cur, sid, obs, val)
                    insert_count += 1
                    series_seen.add(sid)

        print(f"✓ Loaded {insert_count:,} new rows from {latest.name}")

        if insert_count > 0:
            subject = "Baker Hughes loader: Success"
            body = (
                f"Parsed file: {latest.name}\n"
                f"Inserted {insert_count:,} new records across {len(series_seen)} unique series."
            )
            send_email(subject=subject, body=body, to=USER_EMAIL)

        # ✅ Suppress "no new data" emails — just log
        else:
            print(f"No new rows to insert from {latest.name}. All obs_dates already present.")

    except Exception as e:
        send_email(
            subject="Baker Hughes loader: Failed",
            body=f"Error during load: {str(e)}",
            to=USER_EMAIL
        )
        raise

if __name__ == "__main__":
    main()
