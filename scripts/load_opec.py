#!/usr/bin/env python
"""
Parse latest OPEC MOMR Excel file and load selected tables into core_energy.fact_series_value.
"""

import os
from pathlib import Path
from datetime import datetime, date
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from src.utils import send_email

# ────────────── Config ────────────── #
load_dotenv()
PG_DSN     = os.getenv("PG_DSN")
RAW_DIR    = Path(__file__).resolve().parent.parent / "data/raw/opec_reports"
USER_EMAIL = "jarviswilliamd@gmail.com"

MIN_PARTITION_DATE = date(1980, 1, 1)
MAX_PARTITION_DATE = date(2000, 1, 1)

# ────────────── Canonical Mapping ────────────── #
def load_canonical_mappings():
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT series_code FROM core_energy.dim_series")
            allowed_set = set(r[0] for r in cur.fetchall())

            cur.execute("SELECT alias_code, series_code FROM core_energy.dim_series_alias")
            alias_map = {alias: series for alias, series in cur.fetchall()}

    return allowed_set, alias_map

# ────────────── Table Parsers ────────────── #
def parse_table_generic(path: Path, sheet: str, key: str, colname: str) -> list[dict]:
    df = pd.read_excel(path, sheet_name=sheet, skiprows=5, header=None)
    df = df.dropna(how="all")[df.columns[1:]]
    df.columns = [colname] + df.iloc[0].tolist()[1:]
    df = df.iloc[1:]
    df[colname] = df[colname].ffill()
    df = df.melt(id_vars=colname, var_name="obs_date", value_name="value")
    df["obs_date"] = pd.to_datetime(df["obs_date"], errors="coerce").dt.date
    df = df.dropna(subset=["obs_date", "value"])

    return [
        {
            "series_code": f"opec.{key}." + row[colname].strip().lower().replace(" ", "_"),
            "obs_date": row["obs_date"],
            "value": row["value"]
        }
        for _, row in df.iterrows()
        if MIN_PARTITION_DATE <= row["obs_date"] < MAX_PARTITION_DATE
    ]

# ────────────── Upsert Logic ────────────── #
def upsert_series(cur, series_code: str, obs_date: date, value: float):
    cur.execute("""
        INSERT INTO core_energy.fact_series_meta (series_code, source_id, description)
        VALUES (%s, (SELECT source_id FROM core_energy.dim_source WHERE name='OPEC'), %s)
        ON CONFLICT (series_code) DO NOTHING;
    """, (series_code, series_code))

    cur.execute("SELECT series_id FROM core_energy.fact_series_meta WHERE series_code = %s", (series_code,))
    sid_row = cur.fetchone()
    if not sid_row:
        return
    series_id = sid_row[0]

    cur.execute("""
        INSERT INTO core_energy.fact_series_value (series_id, obs_date, value, loaded_at_ts)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (series_id, obs_date) DO NOTHING;
    """, (series_id, obs_date, value, datetime.utcnow()))

# ────────────── Main ────────────── #
def main():
    try:
        files = sorted(RAW_DIR.glob("opec_*.xlsx"))
        if not files:
            send_email(subject="OPEC loader: Failed", body="No OPEC Excel files found.", to=USER_EMAIL)
            return

        allowed_set, alias_map = load_canonical_mappings()
        latest = files[-1]
        print(f"📄 Parsing {latest.name}")

        total_records = 0
        failures = []
        unknown_series = set()

        with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT m.series_code, v.obs_date
                FROM core_energy.fact_series_value v
                JOIN core_energy.fact_series_meta m USING (series_id)
                WHERE m.series_code LIKE 'opec.table11_%'
            """)
            existing = set(cur.fetchall())

            tables = [
                ("Table 11 - 1", "table11_1", "region"),
                ("Table 11 - 3", "table11_3", "category"),
                ("Table 11 - 4", "table11_4", "country")
            ]

            for sheet, key, colname in tables:
                try:
                    records = parse_table_generic(latest, sheet, key, colname)

                    for r in records:
                        sid = r["series_code"].lower().strip()
                        sid = alias_map.get(sid, sid)
                        r["series_code"] = sid

                    unknown_series |= {r["series_code"] for r in records if r["series_code"] not in allowed_set}
                    if unknown_series:
                        raise ValueError(f"❌ Unknown or unapproved series_code(s): {sorted(unknown_series)}")

                    new_records = [r for r in records if (r["series_code"], r["obs_date"]) not in existing]
                    print(f"→ {sheet}: {len(new_records)} new records")

                    for r in new_records:
                        upsert_series(cur, r["series_code"], r["obs_date"], r["value"])
                    total_records += len(new_records)

                except Exception as e:
                    print(f"❌ Error in {sheet}: {e}")
                    failures.append(sheet)
                    conn.rollback()

        # 📨 Email logic
        if total_records > 0:
            subject = "OPEC loader: Success"
            body = (
                f"Parsed file: {latest.name}\n"
                f"Inserted {total_records:,} new records.\n"
                + (f"Failures in: {', '.join(failures)}" if failures else "No errors.")
            )
            send_email(subject=subject, body=body, to=USER_EMAIL)
        elif failures:
            subject = "OPEC loader: Partial Failure"
            body = (
                f"Parsed file: {latest.name}\n"
                f"Inserted {total_records:,} new records.\n"
                f"Failures in: {', '.join(failures)}"
            )
            send_email(subject=subject, body=body, to=USER_EMAIL)
        else:
            print(f"No new records inserted from {latest.name}. All values already present or out of partition range.")

    except Exception as e:
        send_email(subject="OPEC loader: Failed", body=f"Error during load: {str(e)}", to=USER_EMAIL)
        raise

if __name__ == "__main__":
    main()
