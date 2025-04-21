# --- scripts/load_baker.py ---
import os
import pandas as pd
from datetime import datetime
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

load_dotenv()
PG_DSN = os.getenv("PG_DSN")
RAW_DIR = Path(__file__).resolve().parent.parent / "data/raw/bh_rigcount_reports"
TABLE = "core_energy.fact_series_value"
META = "core_energy.fact_series_meta"


def parse_baker(path: Path) -> pd.DataFrame:
    df = pd.read_excel(
        path,
        sheet_name="NAM Weekly",
        skiprows=10,  # header starts at row 11 (0-based index)
        usecols="A:L",
    )
    df = df.dropna(subset=["US_PublishDate", "Rig Count Value"])
    df = df.rename(columns={"Rig Count Value": "value"})

    df["obs_date"] = pd.to_datetime(df["US_PublishDate"], errors="coerce")
    df["series_code"] = (
        "baker." + df["Country"].str.lower()
        + "." + df["DrillFor"].str.lower()
        + "." + df["Trajectory"].str.lower()
        + "." + df["Basin"].str.lower().str.replace(" ", "_")
    )

    return df[["series_code", "obs_date", "value"]].dropna()


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


def main():
    latest = max(RAW_DIR.glob("bh_rigcount_*.xlsx"))
    print(f"📄 Parsing {latest.name}")
    df = parse_baker(latest)

    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT DISTINCT obs_date FROM {TABLE} WHERE series_id IN (
            SELECT series_id FROM {META} WHERE series_code LIKE 'baker.%'
        )")
        existing_dates = {r[0] for r in cur.fetchall()}

        new_df = df[~df["obs_date"].isin(existing_dates)]
        print(f"✓ {len(new_df)} new records to insert")

        for _, row in new_df.iterrows():
            upsert_series(cur, row["series_code"], row["obs_date"], row["value"])

    print("✓ Baker Hughes load complete")


if __name__ == "__main__":
    main()
