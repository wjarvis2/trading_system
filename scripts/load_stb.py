#!/usr/bin/env python
"""
scripts/load_stb.py
────────────────────────────────────────────────────────────────────────────
Load STB EP‑724 Excel railcar extracts → core_energy.fact_series_value

• Reads *.xlsx dropped in data/raw/stb_railcarloads_reports/
• Extracts 5 metric groups: speed, dwell, cars‑on‑line, holding, stalled
• Canonicalizes series_code:  stb.<metric>.<railroad>.<slug>
• Whitelists series_codes via allowed_series_codes()
• Sends email on success / failure
"""

from __future__ import annotations

import os
import traceback
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv

from src.db_utils import allowed_series_codes
from src.utils    import send_email

# ───────────── Config ─────────────
load_dotenv()
PG_DSN     = os.getenv("PG_DSN")
RAW_DIR    = Path(__file__).resolve().parents[1] / "data/raw/stb_railcarloads_reports"
USER_EMAIL = "jarviswilliamd@gmail.com"

TABLE = "core_energy.fact_series_value"
META  = "core_energy.fact_series_meta"
ALIAS = "core_energy.dim_series_alias"

# ───────────── Utility ─────────────

def slug(text: str) -> str:
    return (str(text).strip()
            .lower()
            .replace(" ", "_")
            .replace(",", "")
            .replace("(", "")
            .replace(")", ""))

def extract_meta(path: Path):
    """Extract railroad + timestamp from filename."""
    try:
        rr, datestr = path.stem.split("_", 1)
        return rr.lower(), datetime.strptime(datestr, "%Y%m%d")
    except Exception:
        return path.stem.lower(), datetime.now()

def safe_xl(path, **kwargs):
    try:
        return pd.read_excel(path, **kwargs)
    except Exception:
        return pd.DataFrame()

def clean(x) -> float | None:
    if pd.isna(x) or str(x).strip() in {"-", "", "n/a", "na"}:
        return None
    return float(str(x).replace(",", "").strip())

# ───────────── Parsers ─────────────

def parse_speed(path, rr, ts):
    df = safe_xl(path, skiprows=4, nrows=8, usecols="A:B", header=None)
    return [(f"stb.speed.{rr}.{slug(r[0])}", ts.date(), clean(r[1])) for _, r in df.iterrows() if clean(r[1])]

def parse_dwell(path, rr, ts):
    df = safe_xl(path, skiprows=14, nrows=11, usecols="A:B", header=None)
    return [(f"stb.dwell.{rr}.{slug(r[0])}", ts.date(), clean(r[1])) for _, r in df.iterrows() if "terminal" not in str(r[0]).lower() and clean(r[1])]

def parse_carsonline(path, rr, ts):
    df = safe_xl(path, skiprows=28, nrows=9, usecols="A:B", header=None)
    return [(f"stb.carsonline.{rr}.{slug(r[0])}", ts.date(), clean(r[1])) for _, r in df.iterrows() if clean(r[1])]

def parse_holding(path, rr, ts):
    causes = ["crew", "power", "other", "total"]
    df = safe_xl(path, skiprows=48, nrows=9, usecols="A:E", header=None)
    rows = []
    for _, r in df.iterrows():
        if pd.isna(r[0]) or "train" in str(r[0]).lower():
            continue
        for i, cause in enumerate(causes, start=1):
            v = clean(r[i]) if i < len(r) else None
            if v is not None:
                rows.append((f"stb.holding.{rr}.{slug(r[0])}.{cause}", ts.date(), v))
    return rows

def parse_stalled(path, rr, ts):
    tags = ["loaded", "empty"]
    df = safe_xl(path, skiprows=61, nrows=9, usecols="A:C", header=None)
    rows = []
    for _, r in df.iterrows():
        if pd.isna(r[0]):
            continue
        for i, tag in enumerate(tags, start=1):
            v = clean(r[i]) if i < len(r) else None
            if v is not None:
                rows.append((f"stb.stalled.{rr}.{slug(r[0])}.{tag}", ts.date(), v))
    return rows

PARSERS = [parse_speed, parse_dwell, parse_carsonline, parse_holding, parse_stalled]

# ───────────── DB helpers ─────────────

def upsert_value(cur, sid, odt, val) -> int:
    cur.execute("""
        INSERT INTO core_energy.fact_series_meta (series_code, source_id, description)
        VALUES (%s, (SELECT source_id FROM core_energy.dim_source WHERE name='STB'), %s)
        ON CONFLICT (series_code) DO NOTHING;
    """, (sid, sid))

    cur.execute("SELECT series_id FROM core_energy.fact_series_meta WHERE series_code=%s", (sid,))
    row = cur.fetchone()
    if not row:
        return 0

    cur.execute(f"""
        INSERT INTO {TABLE} (series_id, obs_date, value, loaded_at_ts)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (series_id, obs_date) DO NOTHING;
    """, (row[0], odt, val, datetime.now(timezone.utc)))
    return cur.rowcount

# ───────────── Main ─────────────

def main():
    try:
        files = sorted(RAW_DIR.glob("*.xlsx"))
        if not files:
            send_email("STB loader: No files", "No STB Excel files found.", to=USER_EMAIL)
            return

        allowed = allowed_series_codes()

        with psycopg2.connect(PG_DSN) as conn:
            alias_map = dict(pd.read_sql("SELECT alias_code, series_code FROM core_energy.dim_series_alias", conn).values)

        total_inserted = 0
        failures = []

        with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT m.series_code, v.obs_date
                FROM core_energy.fact_series_value v
                JOIN core_energy.fact_series_meta m USING (series_id)
            """)
            existing = set(cur.fetchall())

            for f in files:
                rr, ts = extract_meta(f)
                try:
                    for parser in PARSERS:
                        for sid, odt, val in parser(f, rr, ts):
                            sid = alias_map.get(sid, sid)
                            if sid not in allowed:
                                print(f"⚠️  Skipping unapproved series_code: {sid}")
                                continue
                            if (sid, odt) in existing:
                                continue
                            total_inserted += upsert_value(cur, sid, odt, val)
                    conn.commit()
                except Exception as e:
                    failures.append(f.name)
                    conn.rollback()
                    print(f"❌ Failed parsing {f.name}: {e}")
                    traceback.print_exc()

        if total_inserted > 0:
            send_email("STB loader: Success", f"Inserted {total_inserted:,} rows. Failures: {failures or 'None'}", to=USER_EMAIL)
            print(f"✓ Inserted {total_inserted:,} rows")
        elif failures:
            send_email("STB loader: Partial", f"Files failed: {failures}", to=USER_EMAIL)
            print(f"⚠️ Partial success — some files failed: {failures}")
        else:
            print("No new rows inserted (up-to-date or filtered)")

    except Exception as exc:
        send_email("STB loader: Failed", body=f"Error during load: {exc}", to=USER_EMAIL)
        raise

if __name__ == "__main__":
    main()
