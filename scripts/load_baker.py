#!/usr/bin/env python
"""
scripts/load_baker.py
────────────────────────────────────────────────────────────────────────────
Load Baker Hughes NAM Weekly workbook → core_energy.fact_series_value

* Reads newest bh_rigcount_*.xlsx in data/raw/bh_rigcount_reports/
* Canonical series_code: baker.<country>.<basin>.<drillfor>
    – spaces OR hyphens ⇒ underscore
* Drops every basin/trajectory not in the 66-series whitelist
* Whitelists against dim_series / dim_series_alias
* Inserts meta rows on-the-fly, bulk-inserts values
* Emails success / failure via src.utils.send_email
"""
from __future__ import annotations

import os
import re
from datetime import datetime, UTC
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv

from src.db_utils import allowed_series_codes          # whitelist helper
from src.utils    import send_email                    # project-specific email helper

# ────────────────────── Config ──────────────────────
load_dotenv()
PG_DSN      = os.getenv("PG_DSN")
RAW_DIR     = Path(__file__).resolve().parents[1] / "data/raw/bh_rigcount_reports"
TABLE       = "core_energy.fact_series_value"
META        = "core_energy.fact_series_meta"
ALIAS_TABLE = "core_energy.dim_series_alias"
SOURCE_NAME = "Baker Hughes"
USER_EMAIL  = "jarviswilliamd@gmail.com"

# Only the 13 canonical series we want in the DB right now
WANTED = {
    'baker.canada.total',
    'baker.us.total',
    'baker.us.permian.oil',
    'baker.us.permian.gas',
    'baker.us.eagle_ford.oil',
    'baker.us.eagle_ford.gas',
    'baker.us.williston.oil',
    'baker.us.williston.gas',
    'baker.us.dj_niobrara.oil',
    'baker.us.dj_niobrara.gas',
    'baker.us.haynesville.gas',
    'baker.us.marcellus.gas',
    'baker.us.utica.gas',
}

# ────────────────────── Helpers ──────────────────────
def _clean(token: str) -> str:
    """lower-case, trim, replace space OR hyphen with underscore"""
    return re.sub(r"[ \-]+", "_", str(token).strip().lower())

def tidy_baker(df: pd.DataFrame) -> pd.DataFrame:
    """Return dataframe with canonical series_code, obs_date, value."""
    country_map = {
        "united states": "us",
        "u.s.": "us",
        "canada": "canada",
    }
    df["country"] = (
        df["Country"]
        .str.strip()
        .str.lower()
        .map(lambda c: country_map.get(c, c))  # fallback = itself
    )

    basin = (
        df["Basin"]
        .fillna("total")
        .str.lower()
        .str.replace(r"[ \-]+", "_", regex=True)
    )
    target = df["DrillFor"].fillna("total").str.lower()

    df["series_code"] = "baker." + df["country"] + "." + basin + "." + target
    df["obs_date"] = pd.to_datetime(df["US_PublishDate"]).dt.date
    return df[["series_code", "obs_date", "value"]]



def parse_workbook(path: Path) -> pd.DataFrame:
    raw = (
        pd.read_excel(
            path, sheet_name="NAM Weekly", skiprows=10, usecols="A:L"
        )
        .dropna(subset=["US_PublishDate", "Rig Count Value"])
        .rename(columns={"Rig Count Value": "value"})
    )
    return tidy_baker(raw)

def ensure_meta(cur, series_codes: list[str]) -> None:
    """Ensure each series_code has a row in fact_series_meta."""
    cur.executemany(
        f"""
        INSERT INTO {META} (series_code, source_id, description)
        VALUES (
            %s,
            (SELECT source_id FROM core_energy.dim_source WHERE name = '{SOURCE_NAME}'),
            %s
        )
        ON CONFLICT (series_code) DO NOTHING;
        """,
        [(s, s) for s in series_codes],
    )

# ────────────────────── Main ──────────────────────
def main() -> None:
    try:
        latest = max(RAW_DIR.glob("bh_rigcount_*.xlsx"))
        print(f"📄  Parsing {latest.name}")
        df = parse_workbook(latest)

        # ── 1 · keep only the 13 whitelisted series ───────────────────────────
        df = df[df["series_code"].isin(WANTED)]

        # ── 2 · canonical alias mapping & validation ──────────────────────────
        allowed = allowed_series_codes()                     # from DB
        with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT alias_code, series_code FROM {ALIAS_TABLE}")
            alias_map = {a: s for a, s in cur.fetchall()}

        df["series_code"] = df["series_code"].map(lambda s: alias_map.get(s, s))

        unknown = set(df["series_code"]) - allowed
        if unknown:
            raise ValueError(f"Unmapped Baker series_code(s): {sorted(unknown)}")

        # ── 3 · insert into DB ────────────────────────────────────────────────
        with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
            ensure_meta(cur, df["series_code"].unique().tolist())

            cur.executemany(
                f"""
                INSERT INTO {TABLE} (series_id, obs_date, value, loaded_at_ts)
                SELECT series_id, %s, %s, %s
                FROM   {META}
                WHERE  series_code = %s
                ON CONFLICT (series_id, obs_date) DO NOTHING;
                """,
                [
                    (obs_date, float(value), datetime.now(UTC), series_code)
                    for series_code, obs_date, value in df.itertuples(index=False)
                ],
            )
            rows_inserted = cur.rowcount

        print(f"✓  Inserted {rows_inserted:,} new rows ({df['series_code'].nunique()} series)")

        if rows_inserted:
            send_email(
                subject="Baker loader: Success",
                body=(
                    f"Parsed file : {latest.name}\n"
                    f"Rows loaded : {rows_inserted:,}\n"
                    f"Series      : {df['series_code'].nunique()}"
                ),
                to=USER_EMAIL,
            )

    except Exception as exc:
        send_email(
            subject="Baker loader: FAILED",
            body=f"Error during load\n\n{exc}",
            to=USER_EMAIL,
        )
        raise

if __name__ == "__main__":
    main()
