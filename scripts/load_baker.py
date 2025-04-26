#!/usr/bin/env python
# --- scripts/load_baker.py ---
from __future__ import annotations
import os
from datetime import datetime, UTC
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv

from src.utils import send_email

# ────────────── Config ──────────────
load_dotenv()
PG_DSN     = os.getenv("PG_DSN")
RAW_DIR    = Path(__file__).resolve().parents[1] / "data/raw/bh_rigcount_reports"
TABLE      = "core_energy.fact_series_value"
META       = "core_energy.fact_series_meta"
USER_EMAIL = "jarviswilliamd@gmail.com"

# ────────────── Helper ──────────────
def make_headline(df: pd.DataFrame, by: list[str]) -> pd.DataFrame:
    headline = (
        df.groupby(["Country", "obs_date"], as_index=False)
          .agg({"value": "sum"})
    )
    headline["DrillFor"] = "total"
    headline["Trajectory"] = "total"
    headline["Basin"] = "total"
    return headline


# ────────────── Canonical Mapping ──────────────
def load_canonical_mappings():
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT series_code FROM core_energy.dim_series")
            allowed_set = set(r[0] for r in cur.fetchall())

            cur.execute("SELECT alias_code, series_code FROM core_energy.dim_series_alias")
            alias_map = {alias: series for alias, series in cur.fetchall()}

    return allowed_set, alias_map



# ────────────── Parser ──────────────
def parse_baker(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(
        path,
        sheet_name="NAM Weekly",
        skiprows=10,
        usecols="A:L",
    ).dropna(subset=["US_PublishDate", "Rig Count Value"])

    raw = raw.rename(columns={"Rig Count Value": "value"})
    raw["obs_date"] = pd.to_datetime(raw["US_PublishDate"]).dt.date

    headline = make_headline(raw, by=["Country"])
    tidy = pd.concat([raw, headline], ignore_index=True)

    tidy["series_code"] = (
        "baker." + tidy["Country"].str.lower()
        + "." + tidy["DrillFor"].fillna("total").str.lower()
        + "." + tidy["Trajectory"].fillna("total").str.lower()
        + "." + tidy["Basin"].fillna("total").str.lower().str.replace(" ", "_")
    )

    return tidy[["series_code", "obs_date", "value"]]

# ────────────── Upsert helpers ──────────────
def upsert_series(cur, series_code: str, obs_date, value: float):
    cur.execute(f"""
        INSERT INTO {META}(series_code, source_id, description)
        VALUES (
            %s,
            (SELECT source_id FROM core_energy.dim_source WHERE name = 'Baker Hughes'),
            %s
        )
        ON CONFLICT (series_code) DO NOTHING;
    """, (series_code, series_code))

    cur.execute(f"SELECT series_id FROM {META} WHERE series_code = %s", (series_code,))
    series_id = cur.fetchone()[0]

    cur.execute(f"""
        INSERT INTO {TABLE}(series_id, obs_date, value, loaded_at_ts)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (series_id, obs_date) DO NOTHING;
    """, (series_id, obs_date, value, datetime.now(UTC)))

# ────────────── Main ──────────────
def main() -> None:
    try:
        latest = max(RAW_DIR.glob("bh_rigcount_*.xlsx"))
        print(f"📄  Parsing {latest.name}")
        df = parse_baker(latest)

        allowed_set, alias_map = load_canonical_mappings()

        with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
            cur.execute(f"""
                SELECT m.series_code, v.obs_date
                FROM {TABLE} v
                JOIN {META} m USING (series_id)
                WHERE m.series_code LIKE 'baker.%'
            """)
            existing = set(cur.fetchall())

            unknown_series = set()
            rows_inserted = 0
            seen_series   = set()

            for series_code, obs_date, value in df.itertuples(index=False):
                sid = series_code.lower().strip()
                sid = alias_map.get(sid, sid)

                if sid not in allowed_set:
                    unknown_series.add(sid)
                    continue

                if (sid, obs_date) not in existing:
                    upsert_series(cur, sid, obs_date, value)
                    rows_inserted += 1
                    seen_series.add(sid)

        if unknown_series:
            raise ValueError(f"Unknown series_code(s): {sorted(unknown_series)}")

        print(f"✓  Inserted {rows_inserted:,} new rows ({len(seen_series)} series)")

        if rows_inserted:
            send_email(
                subject="Baker Hughes loader: Success",
                body=(
                    f"Parsed:  {latest.name}\n"
                    f"Rows:    {rows_inserted:,}\n"
                    f"Series:  {len(seen_series)}"
                ),
                to=USER_EMAIL,
            )

    except Exception as exc:
        send_email(
            subject="Baker Hughes loader: FAILED",
            body=f"Error during load\n\n{exc}",
            to=USER_EMAIL,
        )
        raise

if __name__ == "__main__":
    main()
