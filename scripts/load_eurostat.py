#!/usr/bin/env python
"""
Load Eurostat .tsv.gz files into core_energy.fact_series_value.

- Parses Eurostat tab-separated wide-format files
- Cleans and reshapes to long form
- Handles Eurostat-style nulls
- Inserts only new obs_date rows per series_code
- Processes only the most recent version of each table
"""

import os
import gc
import time
from pathlib import Path
from datetime import datetime, timezone, date
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()
PG_DSN = os.getenv("PG_DSN")
RAW_DIR = Path(__file__).resolve().parent.parent / "data/raw/eurostat_reports"

def get_latest_loaded_dates(cur) -> dict[str, date]:
    cur.execute("""
        SELECT m.series_code, MAX(v.obs_date)
        FROM core_energy.fact_series_value v
        JOIN core_energy.fact_series_meta m USING(series_id)
        GROUP BY m.series_code
    """)
    return dict(cur.fetchall())

def parse_file(path: Path, latest_seen: dict[str, date]) -> list[dict]:
    df = pd.read_csv(
        path, sep="\t", compression="gzip",
        engine="python", dtype=str
    )
    df.columns = df.columns.str.strip()
    df = df.apply(
        lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x)
        if col.dtype == "object" else col
    )
    df.replace({":": pd.NA, ": ": pd.NA, " ": pd.NA, "": pd.NA}, inplace=True)

    df = df.melt(id_vars=df.columns[0], var_name="obs_date", value_name="value")
    df = df[df["value"].notna()].copy()
    df["obs_date"] = pd.to_datetime(df["obs_date"], errors="coerce").dt.date
    df = df.dropna(subset=["obs_date"])

    meta_col = df.columns[0]
    split = df[meta_col].str.split(",", expand=True)
    while split.shape[1] < 4:
        split[split.shape[1]] = None
    df[["freq", "nrg_bal", "indic_nrg", "geo"]] = split.iloc[:, :4]

    dataset_id = path.stem.split("_")[0]
    records = []
    for _, row in df.iterrows():
        sid = f"eurostat.{dataset_id}.{row['nrg_bal']}.{row['indic_nrg']}.{row['geo']}"
        if sid in latest_seen and row["obs_date"] <= latest_seen[sid]:
            continue
        try:
            val = float(row["value"])
        except ValueError:
            continue
        records.append({
            "series_code": sid,
            "obs_date": row["obs_date"],
            "value": val,
        })

    return records

def upsert_many(cur, records: list[dict]):
    if not records:
        return

    unique_codes = set(r["series_code"] for r in records)
    for code in unique_codes:
        cur.execute("""
            INSERT INTO core_energy.fact_series_meta (series_code, source_id, description)
            VALUES (%s, (SELECT source_id FROM core_energy.dim_source WHERE name='Eurostat'), %s)
            ON CONFLICT (series_code) DO NOTHING;
        """, (code, code))

    to_insert = [
        (r["series_code"], r["obs_date"], r["value"], datetime.now(timezone.utc))
        for r in records
    ]

    execute_values(cur, """
        WITH new_values (series_code, obs_date, value, loaded_at_ts) AS (
            VALUES %s
        )
        INSERT INTO core_energy.fact_series_value (series_id, obs_date, value, loaded_at_ts)
        SELECT m.series_id, v.obs_date, v.value, v.loaded_at_ts
        FROM new_values v
        JOIN core_energy.fact_series_meta m ON m.series_code = v.series_code
        ON CONFLICT (series_id, obs_date, loaded_at_ts) DO NOTHING;
    """, to_insert)

def main():
    # Only parse most recent file for each dataset ID
    all_files = sorted(RAW_DIR.glob("*.tsv.gz"))
    latest_per_table = defaultdict(Path)
    for f in all_files:
        table_id = f.name.split("_")[0]
        if table_id not in latest_per_table or f > latest_per_table[table_id]:
            latest_per_table[table_id] = f
    files = list(latest_per_table.values())

    print(f"🔍 Found {len(files)} Eurostat files (latest only)")

    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        latest_seen = get_latest_loaded_dates(cur)
        for file in files:
            print(f"→ Parsing {file.name}...")
            start = time.time()
            try:
                records = parse_file(file, latest_seen)
                upsert_many(cur, records)
                print(f"  ✓ {file.name} → {len(records):,} new rows in {time.time() - start:.1f}s")
            except Exception as e:
                print(f"  ✗ Failed to parse {file.name}: {e}")
            finally:
                del records
                gc.collect()

if __name__ == "__main__":
    main()
