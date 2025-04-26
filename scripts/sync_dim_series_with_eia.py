#!/usr/bin/env python
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ────────────── Config ────────────── #
load_dotenv()
PG_DSN     = os.environ["PG_DSN"]
CSV_PATH   = "config/eia_series.csv"

# ────────────── Main ────────────── #
def main():
    df = pd.read_csv(CSV_PATH, dtype=str)
    df["series_id"] = df["series_id"].str.strip().str.lower()
    df["model_col"] = df["series_id"].str.replace(".", "_", regex=False)
    df["unit"] = "unknown"
    df["freq"] = "unknown"
    df["description"] = df["description"].fillna("")

    engine = create_engine(PG_DSN)
    with engine.begin() as conn:
        existing = pd.read_sql("SELECT series_code FROM core_energy.dim_series", conn)
        existing_codes = set(existing["series_code"].str.lower())

        new_rows = df[~df["series_id"].isin(existing_codes)]
        print(f"🆕 {len(new_rows)} new EIA series to insert into dim_series")

        for _, row in new_rows.iterrows():
            conn.execute(text("""
                INSERT INTO core_energy.dim_series (model_col, series_code, unit, freq, description)
                VALUES (:model_col, :series_id, :unit, :freq, :description)
                ON CONFLICT (model_col) DO UPDATE
                SET series_code = EXCLUDED.series_code,
                    unit        = EXCLUDED.unit,
                    freq        = EXCLUDED.freq,
                    description = EXCLUDED.description;

            """), row.to_dict())

if __name__ == "__main__":
    main()
