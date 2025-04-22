#!/usr/bin/env python
# --- scripts/load_paj.py ---
import os
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import psycopg2
from dotenv import load_dotenv

from src.utils import send_email  # assumes working send_email util

# ────────────────────── Config ────────────────────── #
load_dotenv()
PG_DSN = os.getenv("PG_DSN")
USER_EMAIL = "jarviswilliamd@gmail.com"
RAW_DIR = Path(__file__).resolve().parent.parent / "data/raw/paj_cruderuns_reports"
TABLE = "core_energy.fact_series_value"
META = "core_energy.fact_series_meta"

# ────────────────────── DB Write ────────────────────── #
def upsert_series(cur, series_code: str, obs_date: datetime, value: float):
    if pd.isna(obs_date):
        return
    cur.execute(f"""
        INSERT INTO {META} (series_code, source_id, description)
        VALUES (%s, (SELECT source_id FROM core_energy.dim_source WHERE name='PAJ'), %s)
        ON CONFLICT (series_code) DO NOTHING;
    """, (series_code, series_code))

    cur.execute(f"SELECT series_id FROM {META} WHERE series_code = %s", (series_code,))
    series_id = cur.fetchone()[0]

    cur.execute(f"""
        INSERT INTO {TABLE} (series_id, obs_date, value, loaded_at_ts)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (series_id, obs_date, loaded_at_ts) DO NOTHING;
    """, (series_id, obs_date, value, datetime.now(timezone.utc)))

# ────────────────────── Parsers ────────────────────── #
def parse_crude(path: Path) -> list[dict]:
    df = pd.read_excel(path, sheet_name="Crude Oil", header=3)
    df = df.rename(columns={df.columns[0]: "month"})
    df["month"] = pd.to_datetime(df["month"].astype(str), format="%Y.%m", errors="coerce")
    df = df.dropna(subset=["month"])

    COLUMN_MAP = [
        "production",
        "import",
        "non_refining_use",
        "refinery_throughput",
        "refining_capacity",
        "utilization_pct",
        "end_inventory"
    ]

    records = []
    for idx, colname in enumerate(COLUMN_MAP):
        col = df.columns[idx + 1]
        for _, row in df.iterrows():
            if pd.isna(row[col]): continue
            sid = f"paj.crude.{colname}"
            records.append({"series_code": sid, "obs_date": row["month"], "value": row[col]})
    return records

def parse_products(path: Path) -> list[dict]:
    sheet_map = {
        "1.Production": "production",
        "2.Import": "import",
        "3.Sales": "sales",
        "4.Export": "export",
        "5.Inventory": "inventory"
    }

    PRODUCT_COLUMNS = [
        "gasoline", "naphtha", "jet_fuel", "kerosene", "gas_oil",
        "fuel_oil_a", "fuel_oil_bc", "fuel_oil_total", "product_subtotal",
        "lubricating_oil", "asphalt", "paraffin_wax"
    ]

    records = []
    for sheet, category in sheet_map.items():
        df = pd.read_excel(path, sheet_name=sheet, skiprows=2)
        df = df.rename(columns={df.columns[0]: "month"})
        df["month"] = pd.to_datetime(df["month"].astype(str), format="%Y.%m", errors="coerce")
        df = df.dropna(subset=["month"])

        for idx, product in enumerate(PRODUCT_COLUMNS):
            col = df.columns[idx + 1]
            for _, row in df.iterrows():
                if pd.isna(row[col]): continue
                sid = f"paj.products.{category}.{product}"
                records.append({"series_code": sid, "obs_date": row["month"], "value": row[col]})
    return records

def parse_prices(path: Path) -> list[dict]:
    sheet_map = {
        "1.Volume": "volume",
        "3.Dollars": "dollars"
    }

    IMPORT_COLUMNS = [
        "crude_oil", "gasoline", "naphtha", "kerosene",
        "gas_oil", "fuel_oil_a", "fuel_oil_c"
    ]

    records = []
    for sheet, category in sheet_map.items():
        df = pd.read_excel(path, sheet_name=sheet, skiprows=8)
        df = df.replace("-", pd.NA)
        df = df.rename(columns={df.columns[0]: "month"})
        df["month"] = pd.to_datetime(df["month"].astype(str), format="%Y.%m", errors="coerce")
        df = df.dropna(subset=["month"])

        if category == "dollars":
            df = df.drop(columns=[df.columns[1]])  # Drop FX rate

        for idx, product in enumerate(IMPORT_COLUMNS):
            col = df.columns[idx + 1]
            for _, row in df.iterrows():
                if pd.isna(row[col]): continue
                sid = f"paj.prices.{category}.{product}"
                records.append({"series_code": sid, "obs_date": row["month"], "value": row[col]})
    return records

def parse_stockpiles(path: Path) -> list[dict]:
    STOCKPILE_COLUMNS = [
        "target_days_private", "private_crude_oil", "private_products", "private_equivalent",
        "private_days", "gov_crude_oil", "gov_products", "gov_equivalent", "gov_days",
        "joint_crude_oil", "joint_equivalent", "joint_days", "total_volume", "total_days"
    ]

    df = pd.read_excel(path, sheet_name="epaj-5", skiprows=67, header=None, engine="xlrd")
    df.columns = ["month"] + STOCKPILE_COLUMNS
    df = df.dropna(subset=["month"])

    df["month"] = df["month"].astype(str).str.extract(r"(\d{4})\D*(\d{1,2})").astype(str).agg(".".join, axis=1)
    df["month"] = pd.to_datetime(df["month"], format="%Y.%m", errors="coerce")
    df = df.dropna(subset=["month"])

    records = []
    for col in STOCKPILE_COLUMNS:
        for _, row in df.iterrows():
            if pd.isna(row[col]): continue
            sid = f"paj.stockpile.{col}"
            records.append({"series_code": sid, "obs_date": row["month"], "value": row[col]})
    return records

# ────────────────────── Main Entry ────────────────────── #
def main():
    files = sorted(RAW_DIR.glob("*.xls*"))
    print(f"📥 Loading {len(files)} PAJ Excel files...")

    all_records = []
    failures = []

    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        # 🧠 Fetch all existing (series_code, obs_date)
        cur.execute("""
            SELECT m.series_code, v.obs_date
            FROM core_energy.fact_series_value v
            JOIN core_energy.fact_series_meta m USING (series_id)
        """)
        existing = set(cur.fetchall())

        for f in files:
            print(f"→ {f.name}")
            try:
                if "crude_sd" in f.name:
                    records = parse_crude(f)
                elif "product_sd" in f.name:
                    records = parse_products(f)
                elif "import_price" in f.name:
                    records = parse_prices(f)
                elif "stockpiling" in f.name:
                    records = parse_stockpiles(f)
                else:
                    print("  • Skipped (unknown file type)")
                    continue

                new_records = [
                    r for r in records
                    if (r["series_code"], r["obs_date"]) not in existing
                ]

                for r in new_records:
                    upsert_series(cur, r["series_code"], r["obs_date"], r["value"])
                all_records.extend(new_records)

                print(f"  ✓ Inserted {len(new_records)} new records")

            except Exception as e:
                print(f"  ⚠️ Failed to load {f.name}: {e}")
                failures.append(f.name)

    subject = "PAJ Loader: Success ✅" if not failures else "PAJ Loader: Partial Failure ⚠️"
    body = (
        f"✓ Loaded {len(all_records)} new records from {len(files)} files.\n"
        + ("No errors." if not failures else f"Failures: {', '.join(failures)}")
    )
    send_email(subject=subject, body=body, to=USER_EMAIL)

if __name__ == "__main__":
    main()
