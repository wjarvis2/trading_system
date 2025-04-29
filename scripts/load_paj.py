#!/usr/bin/env python
"""
scripts/load_paj.py
────────────────────────────────────────────────────────────────────────────
Load Petroleum Association of Japan (PAJ) workbooks → core_energy.fact_series_value

• Reads all files in data/raw/paj_cruderuns_reports/
• Supports 4 types: crude_sd, product_sd, import_price, stockpiling
• Canonicalizes series_code: paj.crude.*, paj.products.*, paj.prices.*, paj.stockpile.*
• Whitelists against dim_series.csv
• Auto-handles new months and missing data safely
• E-mails success or failure
"""

from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import psycopg2
from dotenv import load_dotenv

from src.db_utils import allowed_series_codes
from src.utils import send_email

# ───────────── Config ─────────────
load_dotenv()
PG_DSN = os.getenv("PG_DSN")
RAW_DIR = Path(__file__).resolve().parents[1] / "data/raw/paj_cruderuns_reports"
USER_EMAIL = "jarviswilliamd@gmail.com"

TABLE = "core_energy.fact_series_value"
META = "core_energy.fact_series_meta"
ALIAS = "core_energy.dim_series_alias"

# ───────────── Helpers ─────────────

def upsert_value(cur, sid: str, obs, val: float) -> int:
    cur.execute("""
        INSERT INTO core_energy.fact_series_meta (series_code, source_id, description)
        VALUES (%s, (SELECT source_id FROM core_energy.dim_source WHERE name='PAJ'), %s)
        ON CONFLICT (series_code) DO NOTHING;
    """, (sid, sid))

    cur.execute("SELECT series_id FROM core_energy.fact_series_meta WHERE series_code=%s", (sid,))
    row = cur.fetchone()
    if not row:
        return 0

    series_id = row[0]
    cur.execute("""
        INSERT INTO core_energy.fact_series_value (series_id, obs_date, value, loaded_at_ts)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (series_id, obs_date) DO NOTHING;
    """, (series_id, obs, val, datetime.now(timezone.utc)))
    return cur.rowcount

def parse_crude(path: Path) -> list[dict]:
    df = pd.read_excel(path, sheet_name="Crude Oil", header=3)
    df.rename(columns={df.columns[0]: "month"}, inplace=True)
    df["month"] = pd.to_datetime(df["month"], format="%Y.%m", errors="coerce")
    df = df.dropna(subset=["month"])

    col_map = [
        "production", "import", "non_refining_use", "refinery_throughput",
        "refining_capacity", "utilization_pct", "end_inventory"
    ]
    records = []
    for idx, metric in enumerate(col_map, start=1):
        for m, v in zip(df["month"], df.iloc[:, idx]):
            if pd.notna(v):
                records.append({"series_code": f"paj.crude.{metric}", "obs_date": m.date(), "value": v})
    return records

def parse_products(path: Path) -> list[dict]:
    sheet_cat = {
        "1.Production": "production",
        "2.Import": "import",
        "3.Sales": "sales",
        "4.Export": "export",
        "5.Inventory": "inventory"
    }
    products = [
        "gasoline", "naphtha", "jet_fuel", "kerosene", "gas_oil",
        "fuel_oil_a", "fuel_oil_bc", "fuel_oil_total", "product_subtotal",
        "lubricating_oil", "asphalt", "paraffin_wax"
    ]
    records = []
    for sheet, cat in sheet_cat.items():
        df = pd.read_excel(path, sheet_name=sheet, skiprows=2)
        df.rename(columns={df.columns[0]: "month"}, inplace=True)
        df["month"] = pd.to_datetime(df["month"], format="%Y.%m", errors="coerce")
        df = df.dropna(subset=["month"])

        for idx, prod in enumerate(products, start=1):
            for m, v in zip(df["month"], df.iloc[:, idx]):
                if pd.notna(v):
                    records.append({"series_code": f"paj.products.{cat}.{prod}", "obs_date": m.date(), "value": v})
    return records

def parse_prices(path: Path) -> list[dict]:
    sheet_cat = {"1.Volume": "volume", "3.Dollars": "dollars"}
    products  = ["crude_oil", "gasoline", "naphtha", "kerosene", "gas_oil", "fuel_oil_a", "fuel_oil_c"]
    records = []
    for sheet, cat in sheet_cat.items():
        df = pd.read_excel(path, sheet_name=sheet, skiprows=8)
        df.replace("-", pd.NA, inplace=True)
        df.rename(columns={df.columns[0]: "month"}, inplace=True)
        df["month"] = pd.to_datetime(df["month"], format="%Y.%m", errors="coerce")
        df = df.dropna(subset=["month"])

        if cat == "dollars":
            df = df.drop(columns=[df.columns[1]])  # drop FX if present

        for idx, prod in enumerate(products, start=1):
            for m, v in zip(df["month"], df.iloc[:, idx]):
                if pd.notna(v):
                    records.append({"series_code": f"paj.prices.{cat}.{prod}", "obs_date": m.date(), "value": v})
    return records

def parse_stockpile(path: Path) -> list[dict]:
    columns = [
        "target_days_private", "private_crude_oil", "private_products", "private_equivalent", "private_days",
        "gov_crude_oil", "gov_products", "gov_equivalent", "gov_days",
        "joint_crude_oil", "joint_equivalent", "joint_days",
        "total_volume", "total_days"
    ]
    df = pd.read_excel(path, sheet_name="epaj-5", skiprows=7, header=None)
    df.dropna(subset=[0], inplace=True)

    current_year = None
    records = []
    for _, row in df.iterrows():
        cell = str(row[0]).strip().replace("\u3000", " ")
        if " " in cell:
            parts = cell.split()
            if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
                current_year = int(parts[0])
                month = int(parts[1])
            else:
                continue
        else:
            if current_year is None or not cell.isdigit():
                continue
            month = int(cell)

        try:
            obs = datetime(current_year, month, 1).date()
        except Exception:
            continue

        for idx, colname in enumerate(columns, start=1):
            val = row[idx] if idx < len(row) else None
            if pd.notna(val):
                records.append({"series_code": f"paj.stockpile.{colname}", "obs_date": obs, "value": val})
    return records

type_parsers = {
    "crude_sd": parse_crude,
    "product_sd": parse_products,
    "import_price": parse_prices,
    "stockpiling": parse_stockpile
}

# ───────────── Main ─────────────

def main() -> None:
    try:
        files = sorted(RAW_DIR.glob("*.xls*"))
        if not files:
            raise FileNotFoundError("No PAJ Excel files found.")
        print(f"📄 Found {len(files)} PAJ files to process")

        allowed = allowed_series_codes()
        with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT alias_code, series_code FROM {ALIAS}")
            alias_map = {a: s for a, s in cur.fetchall()}

        unknown = set()
        total_new = 0

        with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
            for f in files:
                print(f"→ {f.name}")
                ptype = next((k for k in type_parsers if k in f.name), None)
                if not ptype:
                    print("   • Skipped (unrecognized file type)")
                    continue
                records = type_parsers[ptype](f)

                for r in records:
                    r["series_code"] = alias_map.get(r["series_code"], r["series_code"])
                    if r["series_code"] not in allowed:
                        unknown.add(r["series_code"])
                        continue
                    total_new += upsert_value(cur, r["series_code"], r["obs_date"], r["value"])

            if unknown:
                print(f"⚠️ Skipped unapproved PAJ series: {sorted(unknown)}")

        # email summary
        if total_new:
            send_email(subject="PAJ loader: Success",
                       body=f"Inserted {total_new:,} new rows from {len(files)} files",
                       to=USER_EMAIL)
            print(f"✓ Inserted {total_new:,} new rows")
        else:
            print("No new rows inserted (already up-to-date)")

    except Exception as exc:
        send_email(subject="PAJ loader: Failed",
                   body=f"Error during load: {exc}",
                   to=USER_EMAIL)
        raise

if __name__ == "__main__":
    main()
