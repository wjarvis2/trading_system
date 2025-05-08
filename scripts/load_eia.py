#!/usr/bin/env python
"""
scripts/load_eia.py
────────────────────────────────────────────────────────────────────────────
Load curated EIA CSV snapshot → core_energy.fact_series_value

• Reads latest eia_*.csv in data/raw/eia_reports/
• Canonicalizes series_code via dim_series_alias (optional)
• Skips anything not in allowed_series_codes
• Computes pet.calc.crude_adjustment from other fields
• Bulk-inserts new obs (FK‑safe)
• Sends success / failure email
"""

from __future__ import annotations
import os, sys
from pathlib import Path
from datetime import datetime, UTC
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from src.db_utils import allowed_series_codes
from src.utils import send_email

# ───────────── Config ─────────────
load_dotenv()
PG_DSN     = os.getenv("PG_DSN")
USER_EMAIL = "jarviswilliamd@gmail.com"
ROOT_DIR   = Path(__file__).resolve().parents[1]
RAW_DIR    = ROOT_DIR / "data/raw/eia_reports"
SERIES_META = ROOT_DIR / "config/eia_series.csv"

TABLE     = "core_energy.fact_series_value"
META      = "core_energy.fact_series_meta"
ALIAS_TBL = "core_energy.dim_series_alias"

ADJUSTMENT_CODE = "pet.calc.crude_adjustment"

# ───────────── Helpers ─────────────
def latest_snapshot() -> Path:
    files = sorted(RAW_DIR.glob("eia_*.csv"))
    if not files:
        sys.exit("❌ No snapshot CSVs found in data/raw/eia_reports/")
    return files[-1]

def series_description_lookup() -> dict[str, str]:
    if SERIES_META.exists():
        df = pd.read_csv(SERIES_META, dtype=str)
        return df.set_index("series_id")["description"].to_dict()
    return {}

def group_snapshot(csv_path: Path) -> dict[str, pd.DataFrame]:
    df = pd.read_csv(csv_path, dtype={"series_id": str})

    for c in ("date", "period"):
        if c in df.columns:
            df.rename(columns={c: "obs_date"}, inplace=True)

    df["obs_date"] = pd.to_datetime(df["obs_date"], errors="coerce").dt.date
    df["value"]    = pd.to_numeric(df["value"], errors="coerce")

    df = (
        df[df["obs_date"].notna() & df["value"].notna()]
          .sort_values(["series_id", "obs_date"])
          .drop_duplicates(subset=["series_id", "obs_date"], keep="last")
    )

    return {sid: grp[["obs_date", "value"]].copy() for sid, grp in df.groupby("series_id")}

def fetch_alias_map(cur) -> dict[str, str]:
    cur.execute(f"SELECT alias_code, series_code FROM {ALIAS_TBL}")
    return {a.lower(): s for a, s in cur.fetchall()}

def ensure_meta_rows(cur, canonical_codes: set[str]):
    cur.execute(
        f"""
        INSERT INTO {META} (series_code, source_id, description)
        SELECT c, (SELECT source_id FROM core_energy.dim_source WHERE name='EIA'), c
        FROM   (SELECT UNNEST(%s::text[])) AS t(c)
        ON CONFLICT (series_code) DO NOTHING;
        """,
        (list(canonical_codes),),
    )

def insert_values(cur, series_code: str, df: pd.DataFrame):
    cur.execute(f"SELECT series_id FROM {META} WHERE series_code=%s", (series_code,))
    sid = cur.fetchone()[0]
    execute_values(
        cur,
        f"""
        INSERT INTO {TABLE} (series_id, obs_date, value, loaded_at_ts)
        VALUES %s
        ON CONFLICT (series_id, obs_date) DO NOTHING;
        """,
        [(sid, r.obs_date, r.value, datetime.now(UTC)) for r in df.itertuples(index=False)],
    )

def compute_crude_adjustment(grouped: dict[str, pd.DataFrame]) -> pd.DataFrame | None:
    required = [
        "field_production_of_crude",
        "crude_imports_total",
        "crude_exports_total",
        "crude_stock_change_total",
        "crude_runs_to_refineries",
        "transfers_to_crude_supply",
    ]
    if not all(col in grouped for col in required):
        missing = [col for col in required if col not in grouped]
        print(f"⚠️ Cannot compute adjustment — missing: {missing}")
        return None

    df = pd.DataFrame(index=sorted(set().union(*[grouped[col]["obs_date"] for col in required])))
    for col in required:
        df = df.merge(grouped[col].rename(columns={"value": col}), on="obs_date", how="left")

    df["value"] = (
        df["crude_runs_to_refineries"]
      + df["crude_exports_total"]
      + df["crude_stock_change_total"]
      - df["field_production_of_crude"]
      - df["crude_imports_total"]
      - df["transfers_to_crude_supply"]
    )
    return df[["obs_date", "value"]].dropna()

# ───────────── Main ─────────────
def main():
    snap = latest_snapshot()
    print(f"📄  Parsing {snap.name}")

    grouped_raw  = group_snapshot(snap)
    descr_lookup = series_description_lookup()
    whitelist    = allowed_series_codes()

    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        alias_map = fetch_alias_map(cur)

        # Canonicalize and apply whitelist
        grouped: dict[str, pd.DataFrame] = {}
        for raw_code, df in grouped_raw.items():
            canon = alias_map.get(raw_code.lower(), raw_code.lower())
            if canon not in whitelist:
                print(f"⚠️  Skipping unapproved EIA series_code: {canon}")
                continue

            cur.execute("SELECT model_col FROM core_energy.dim_series WHERE series_code = %s", (canon,))
            result = cur.fetchone()
            if not result:
                print(f"⚠️  No model_col found for series_code: {canon}")
                continue

            model_col = result[0]
            grouped[model_col] = df

        print("✅ Canonicalized and loaded model_col keys:", sorted(grouped.keys()))
        print("🧪 Required for adjustment:", [
            "field_production_of_crude",
            "crude_imports_total",
            "crude_exports_total",
            "crude_stock_change_total",
            "crude_runs_to_refineries",
            "transfers_to_crude_supply"
        ])

        adjustment_df = compute_crude_adjustment(grouped)
        if adjustment_df is not None:
            grouped[ADJUSTMENT_CODE] = adjustment_df

        ensure_meta_rows(cur, set(grouped))

        total_rows = 0
        for model_col, df in grouped.items():
            insert_values(cur, model_col, df)
            total_rows += df.shape[0]

    print(f"✓ Loaded {len(grouped)} series ({total_rows:,} new rows) → DB")

    if total_rows:
        send_email(subject="EIA loader: Success",
                   body=f"Snapshot: {snap.name}\nRows: {total_rows:,}",
                   to=USER_EMAIL)
    else:
        print("No new observations; DB already up-to-date.")

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        send_email(subject="EIA loader: FAILED", body=str(exc), to=USER_EMAIL)
        raise
