#!/usr/bin/env python
# --- scripts/load_paj.py ---
import os
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from src.utils import send_email

# ────────────── Config ────────────── #
load_dotenv()
PG_DSN     = os.getenv("PG_DSN")
USER_EMAIL = "jarviswilliamd@gmail.com"
RAW_DIR    = Path(__file__).resolve().parent.parent / "data/raw/paj_cruderuns_reports"
TABLE      = "core_energy.fact_series_value"
META       = "core_energy.fact_series_meta"

# ────────────── Canonical Series Mapping ────────────── #
def load_canonical_mappings():
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT series_code FROM core_energy.dim_series")
            allowed_set = set(r[0] for r in cur.fetchall())

            cur.execute("SELECT alias_code, series_code FROM core_energy.dim_series_alias")
            alias_map = {alias: series for alias, series in cur.fetchall()}

    return allowed_set, alias_map

# ────────────── DB Write ────────────── #
def upsert_series(cur, series_code: str, obs_date: datetime, value: float):
    if pd.isna(obs_date):
        return

    cur.execute(f"""
        INSERT INTO {META} (series_code, source_id, description)
        VALUES (%s, (SELECT source_id FROM core_energy.dim_source WHERE name='PAJ'), %s)
        ON CONFLICT (series_code) DO NOTHING;
    """, (series_code, series_code))

    cur.execute(f"SELECT series_id FROM {META} WHERE series_code = %s", (series_code,))
    sid = cur.fetchone()
    if not sid:
        return
    series_id = sid[0]

    cur.execute(f"""
        INSERT INTO {TABLE} (series_id, obs_date, value, loaded_at_ts)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (series_id, obs_date) DO NOTHING;
    """, (series_id, obs_date, value, datetime.now(timezone.utc)))

# ────────────── Parsers ────────────── #
def parse_crude(path: Path) -> list[dict]:
    df = pd.read_excel(path, sheet_name="Crude Oil", header=3)
    df = df.rename(columns={df.columns[0]: "month"})
    df["month"] = pd.to_datetime(df["month"].astype(str), format="%Y.%m", errors="coerce")
    df = df.dropna(subset=["month"])

    COLUMN_MAP = [
        "production", "import", "non_refining_use", "refinery_throughput",
        "refining_capacity", "utilization_pct", "end_inventory"
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

    df = pd.read_excel(
        path,
        sheet_name="epaj-5",
        skiprows=7,
        nrows=100,
        header=None,
        engine="xlrd"
    )

    # First column is interleaved year/month values like '2023 1', '2', '3'...
    df = df.dropna(subset=[0])  # Drop empty rows
    df = df.reset_index(drop=True)

    parsed_rows = []
    current_year = None

    for _, row in df.iterrows():
        cell = str(row[0]).strip()
        if " " in cell or "\u3000" in cell:  # Handle full-width spaces too
            parts = cell.replace("\u3000", " ").split()
            if len(parts) == 2:
                current_year = int(parts[0])
                month = int(parts[1])
            else:
                continue
        else:
            if current_year is None:
                continue
            month = int(cell)

        try:
            obs_date = datetime(current_year, month, 1).date()

        except Exception:
            continue

        values = row[1:1+len(STOCKPILE_COLUMNS)].values
        if len(values) != len(STOCKPILE_COLUMNS):
            continue

        for col_name, val in zip(STOCKPILE_COLUMNS, values):
            if pd.isna(val):
                continue
            sid = f"paj.stockpile.{col_name}"
            parsed_rows.append({
                "series_code": sid,
                "obs_date": obs_date,
                "value": val
            })

    return parsed_rows

# ────────────── Main Entry ────────────── #
def main():
    try:
        files = sorted(RAW_DIR.glob("*.xls*"))
        if not files:
            send_email(
                subject="PAJ loader: Failed",
                body="No PAJ Excel files found in raw data directory.",
                to=USER_EMAIL
            )
            print("⚠️ No files to process.")
            return

        allowed_set, alias_map = load_canonical_mappings()
        print(f"📥 Loading {len(files)} PAJ Excel files...")

        all_records = []
        failures = []
        unknown_series = set()

        with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
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

                    for r in records:
                        sid = r["series_code"].lower().strip()
                        sid = alias_map.get(sid, sid)
                        r["series_code"] = sid

                    unknown_series |= {r["series_code"] for r in records if r["series_code"] not in allowed_set}

                    if unknown_series:
                        raise ValueError(f"❌ Unknown or unapproved series_code(s): {sorted(unknown_series)}")

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
                    conn.rollback()

        # 📬 Email Summary
        if all_records:
            subject = "PAJ loader: Success"
            body = (
                f"✓ Loaded {len(all_records)} new records from {len(files)} files.\n"
                + ("No errors." if not failures else f"Failures: {', '.join(failures)}")
            )
            send_email(subject=subject, body=body, to=USER_EMAIL)
        elif failures:
            subject = "PAJ loader: Partial Failure"
            body = (
                f"Attempted to load {len(files)} files.\n"
                f"Failures: {', '.join(failures)}"
            )
            send_email(subject=subject, body=body, to=USER_EMAIL)
        else:
            print("No new data to insert. All values already existed.")

    except Exception as e:
        send_email(subject="PAJ loader: Failed", body=str(e), to=USER_EMAIL)
        raise

if __name__ == "__main__":
    main()

