# --- scripts/load_paj.py ---
import os
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()
PG_DSN = os.getenv("PG_DSN")
RAW_DIR = Path(__file__).resolve().parent.parent / "data/raw/paj_cruderuns_reports"
TABLE = "core_energy.fact_series_value"
META = "core_energy.fact_series_meta"

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

def parse_crude(path: Path) -> list[dict]:
    df = pd.read_excel(path, sheet_name="Crude Oil", skiprows=2)
    df = df.dropna(subset=[df.columns[0]])
    df = df.rename(columns={df.columns[0]: "month"})
    df["month"] = pd.to_datetime(df["month"].astype(str), format="%Y.%m", errors="coerce")
    df = df.dropna(subset=["month"])

    records = []
    for col in df.columns[1:]:
        for _, row in df.iterrows():
            if pd.isna(row["month"]) or pd.isna(row[col]): continue
            sid = f"paj.crude.{col.strip().lower().replace(' ', '_')}"
            records.append({"series_code": sid, "obs_date": row["month"], "value": row[col]})
    return records

def parse_products(path: Path) -> list[dict]:
    records = []
    for sheet in ["1.Production", "2.Import", "3.Sales", "4.Export", "5.Inventory"]:
        try:
            df = pd.read_excel(path, sheet_name=sheet, skiprows=2)
            df = df.dropna(subset=[df.columns[0]])
            df = df.rename(columns={df.columns[0]: "month"})
            df["month"] = pd.to_datetime(df["month"].astype(str), format="%Y.%m", errors="coerce")
            df = df.dropna(subset=["month"])
            for col in df.columns[1:]:
                for _, row in df.iterrows():
                    if pd.isna(row["month"]) or pd.isna(row[col]): continue
                    sid = f"paj.products.{sheet.lower().replace('.', '').strip()}.{col.strip().lower().replace(' ', '_')}"
                    records.append({"series_code": sid, "obs_date": row["month"], "value": row[col]})
        except Exception as e:
            print(f"⚠️ Skipped sheet {sheet}: {e}")
    return records

def parse_prices(path: Path) -> list[dict]:
    records = []
    for sheet in ["1.Volume", "3.Dollars"]:
        df = pd.read_excel(path, sheet_name=sheet, skiprows=8)
        df = df.replace("-", pd.NA)
        df = df.dropna(subset=[df.columns[0]])
        df = df.rename(columns={df.columns[0]: "month"})
        df["month"] = pd.to_datetime(df["month"].astype(str), format="%Y.%m", errors="coerce")
        df = df.dropna(subset=["month"])
        for col in df.columns[1:]:
            for _, row in df.iterrows():
                if pd.isna(row["month"]) or pd.isna(row[col]):
                    continue
                sid = f"paj.prices.{sheet.lower().replace('.', '')}.{col.strip().lower().replace(' ', '_')}"
                records.append({"series_code": sid, "obs_date": row["month"], "value": row[col]})
    return records

def parse_stockpiles(path: Path) -> list[dict]:
    df = pd.read_excel(path, sheet_name="epaj-5", skiprows=3, engine="xlrd")
    df = df.dropna(subset=[df.columns[0]])
    df = df.rename(columns={df.columns[0]: "month"})
    df["month"] = df["month"].astype(str).str.extract(r"(\d{4})\D*(\d+)").astype(str).agg(".".join, axis=1)
    df["month"] = pd.to_datetime(df["month"], format="%Y.%m", errors="coerce")
    df = df.dropna(subset=["month"])

    records = []
    for col in df.columns[1:]:
        for _, row in df.iterrows():
            if pd.isna(row["month"]) or pd.isna(row[col]): continue
            sid = f"paj.stockpile.{col.strip().lower().replace(' ', '_')}"
            records.append({"series_code": sid, "obs_date": row["month"], "value": row[col]})
    return records

# ────────────────────── Main Entry ────────────────────── #
def main():
    files = sorted(RAW_DIR.glob("*.xls*"))
    print(f"📥 Loading {len(files)} PAJ Excel files...")

    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
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
                    upsert_series(cur, r["series_code"], r["obs_date"], r["value"])

                print(f"  ✓ Inserted {len(records)} records")
            except Exception as e:
                print(f"  ⚠️ Failed to load {f.name}: {e}")

    print("✓ PAJ loading complete")

if __name__ == "__main__":
    main()
