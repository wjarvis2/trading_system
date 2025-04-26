#!/usr/bin/env python
# --- scripts/load_eia.py ---
import os
from pathlib import Path
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from src.utils import send_email

# ────────────── Config ────────────── #
load_dotenv()
PG_DSN      = os.environ["PG_DSN"]
USER_EMAIL  = "jarviswilliamd@gmail.com"
ROOT_DIR    = Path(__file__).resolve().parent.parent
RAW_DIR     = ROOT_DIR / "data" / "raw" / "eia_reports"
SERIES_CSV  = ROOT_DIR / "config" / "eia_series.csv"
TABLE       = "core_energy.fact_series_value"
META        = "core_energy.fact_series_meta"

# ────────────── Helpers ────────────── #
def latest_snapshot() -> Path:
    snaps = sorted(RAW_DIR.glob("eia_*.csv"))
    if not snaps:
        raise FileNotFoundError("No snapshot files in data/raw/eia_reports/")
    return snaps[-1]

def load_series_lookup() -> dict[str, str]:
    df = pd.read_csv(SERIES_CSV, dtype=str)
    return df.set_index("series_id")["description"].to_dict()

def group_snapshot(path: Path) -> dict[str, pd.DataFrame]:
    df = pd.read_csv(path, dtype={"series_id": str})
    df.rename(columns={c: "obs_date" for c in ["date", "period"] if c in df.columns}, inplace=True)

    df["obs_date"] = pd.to_datetime(df["obs_date"], errors="coerce").dt.date
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df[df["obs_date"].notna() & df["value"].notna()]

    df = df.sort_values(["series_id", "obs_date", "value"])
    df = df.drop_duplicates(subset=["series_id", "obs_date"], keep="last")

    grouped = {
        sid: group[["obs_date", "value"]].copy()
        for sid, group in df.groupby("series_id")
    }
    return grouped

def fetch_existing_obs_dates(cur, series_code: str) -> set:
    cur.execute("SELECT series_id FROM core_energy.fact_series_meta WHERE series_code = %s", (series_code,))
    result = cur.fetchone()
    if not result:
        return set()
    series_id = result[0]
    cur.execute("SELECT obs_date FROM core_energy.fact_series_value WHERE series_id = %s", (series_id,))
    return {row[0] for row in cur.fetchall()}

def upsert_series(cur, series_code: str, description: str, df: pd.DataFrame) -> int:
    cur.execute("""
        INSERT INTO core_energy.fact_series_meta (series_code, source_id, description)
        VALUES (%s, (SELECT source_id FROM core_energy.dim_source WHERE name = 'EIA'), %s)
        ON CONFLICT (series_code) DO NOTHING;
    """, (series_code, description))

    cur.execute("SELECT series_id FROM core_energy.fact_series_meta WHERE series_code = %s", (series_code,))
    result = cur.fetchone()
    if not result:
        return 0

    series_id = result[0]
    existing = fetch_existing_obs_dates(cur, series_code)

    new_records = [
        (series_id, row.obs_date, row.value, datetime.utcnow())
        for row in df.itertuples(index=False)
        if row.obs_date not in existing
    ]

    if not new_records:
        return 0

    execute_values(
        cur,
        f"""
        INSERT INTO {TABLE} (series_id, obs_date, value, loaded_at_ts)
        VALUES %s
        ON CONFLICT (series_id, obs_date) DO NOTHING;
        """,
        new_records
    )
    return len(new_records)

# ────────────── Main ────────────── #
def main():
    try:
        snap = latest_snapshot()
        series_lookup = load_series_lookup()
        grouped = group_snapshot(snap)

        # --- Canonical enforcement ---
        with psycopg2.connect(PG_DSN) as conn:
            alias_df = pd.read_sql("SELECT alias_code, series_code FROM core_energy.dim_series_alias", conn)
            allowed_df = pd.read_sql("SELECT series_code FROM core_energy.dim_series", conn)

        alias_map = alias_df.set_index("alias_code")["series_code"].to_dict()
        allowed_set = set(allowed_df["series_code"])

        grouped_canonical = {}
        for sid, df in grouped.items():
            sid_lower = sid.strip().lower()
            canonical = alias_map.get(sid_lower, sid_lower)
            if canonical not in allowed_set:
                raise ValueError(f"❌ Unapproved or unknown EIA series_code: {canonical}")
            grouped_canonical[canonical] = df

        # --- Load into DB ---
        total_inserted = 0
        with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
            for series_code, df in grouped_canonical.items():
                description = series_lookup.get(series_code, series_code)
                total_inserted += upsert_series(cur, series_code, description, df)

        print(f"✓ Loaded {len(grouped_canonical)} series ({total_inserted:,} new rows) from {snap.name}")

        if total_inserted > 0:
            send_email(
                subject="EIA loader: Success",
                body=f"Inserted {total_inserted:,} new records from {snap.name}",
                to=USER_EMAIL
            )
        else:
            print(f"No new values inserted from {snap.name}. All obs_dates already exist.")

    except Exception as e:
        send_email(
            subject="EIA loader: Failed",
            body=f"Error during load: {str(e)}",
            to=USER_EMAIL
        )
        raise

if __name__ == "__main__":
    main()
