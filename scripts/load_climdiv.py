# --- scripts/load_climdiv.py ---
import os
from pathlib import Path
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()
PG_DSN = os.getenv("PG_DSN")
RAW_DIR = Path(__file__).resolve().parent.parent / "data/raw/climdiv_hddcdd_reports"
TABLE = "core_energy.fact_series_value"
META = "core_energy.fact_series_meta"

MONTH_MAP = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04", "MAY": "05", "JUN": "06",
    "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12"
}

def parse_climdiv(path: Path) -> list[dict]:
    df = pd.read_csv(path)
    df = df.dropna(subset=["value"])

    df["month"] = df["month"].str.upper().map(MONTH_MAP)
    df["obs_date"] = pd.to_datetime(df["year"].astype(str) + "-" + df["month"], format="%Y-%m")
    df["series_code"] = "climdiv." + df["variable"].str.lower() + "." + df["division"]

    return df[["series_code", "obs_date", "value"]].to_dict("records")

def upsert_series(cur, series_code: str, obs_date: datetime, value: float):
    cur.execute(f"""
        INSERT INTO {META} (series_code, source_id, description)
        VALUES (%s, (SELECT source_id FROM core_energy.dim_source WHERE name='CLIMDIV'), %s)
        ON CONFLICT (series_code) DO NOTHING;
    """, (series_code, series_code))

    cur.execute(f"SELECT series_id FROM {META} WHERE series_code = %s", (series_code,))
    series_id = cur.fetchone()[0]

    cur.execute(f"""
        INSERT INTO {TABLE} (series_id, obs_date, value, loaded_at_ts)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (series_id, obs_date, loaded_at_ts) DO NOTHING;
    """, (series_id, obs_date, value, datetime.utcnow()))

def main():
    latest = max(RAW_DIR.glob("climdiv_hddcdd_normalized_*.csv"))
    print(f"📄 Parsing {latest.name}")

    records = parse_climdiv(latest)

    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        for r in records:
            upsert_series(cur, r["series_code"], r["obs_date"], r["value"])

    print(f"✓ Loaded {len(records):,} climdiv rows")

if __name__ == "__main__":
    main()
