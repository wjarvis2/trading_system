#!/usr/bin/env python
"""
scripts/load_wpsr.py
────────────────────────────────────────────────────────────────────────────
Parse every *.csv in data/raw/wpsr_reports/ and upsert the rows we care about
into core_energy.fact_series_value.

• Robust to row-number changes (“(4) Net Imports …” → “(7) …”).
• Ignores any rows not listed in STOCKS_MAP / SUPPLY_MAP.
• Idempotent (ON CONFLICT DO NOTHING).
• Success / failure e-mails via src.utils.send_email.
"""

from __future__ import annotations

import csv
import os
import re
from datetime import datetime, UTC
from pathlib import Path
from typing import Dict, List, Tuple

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from src.utils import send_email

# ───────────── Config ─────────────
load_dotenv()
PG_DSN      = os.getenv("PG_DSN")
USER_EMAIL  = os.getenv("USER_EMAIL", "jarviswilliamd@gmail.com")
ROOT_DIR    = Path(__file__).resolve().parents[1]
RAW_DIR     = ROOT_DIR / "data/raw/wpsr_reports"
TABLE       = "core_energy.fact_series_value"
META        = "core_energy.fact_series_meta"
SOURCE_NAME = "EIA_WPSR"

# ───────────── Helpers ─────────────
LABEL_RE = re.compile(r"^\(\d+\)\s*")         # strip leading “(14) ” etc.

def _clean(s: str) -> str:
    return re.sub(r"\s{2,}", " ", LABEL_RE.sub("", s)).strip()

def _num(x: str | None) -> float | None:
    if x in (None, "", "–", "—", "– –"):
        return None
    try:
        return float(x.replace(",", ""))
    except ValueError:
        return None

# ───────────── Mapping tables ─────────────
# Table-1 stocks
STOCKS_MAP: Dict[str, Tuple[str, str]] = {
    "Crude Oil":                          ("wpsr.ending_total_crude",        "ending_total_crude_stocks"),
    "Commercial (Excluding SPR)":         ("wpsr.ending_commercial_crude",   "ending_commercial_crude_stocks"),
    "Strategic Petroleum Reserve (SPR)":  ("wpsr.ending_spr_crude",          "ending_spr_crude_stocks"),
}

# Table-2 supply / disposition
SUPPLY_MAP: Dict[Tuple[str, str], Tuple[str, str]] = {
    # column-0 (section stub after clean), column-1 (row label after clean)
    ("Crude Oil Supply", "Domestic Production"):            ("wpsr.domestic_production",        "wpsr.domestic_production"),
    ("Crude Oil Supply", "Alaska"):                         ("wpsr.alaska_production",          "wpsr.alaska_production"),
    ("Crude Oil Supply", "Lower 48"):                       ("wpsr.lower_48_production",        "wpsr.lower_48_production"),
    ("Crude Oil Supply", "Transfers to Crude Oil Supply"):  ("wpsr.transfers_to_supply",        "wpsr.transfers_to_supply"),
    ("Crude Oil Supply", "Net Imports (Including SPR)"):    ("wpsr.net_imports",                "wpsr.net_imports"),
    ("Crude Oil Supply", "Imports"):                        ("wpsr.total_imports",              "wpsr.total_imports"),
    ("Crude Oil Supply", "Commercial Crude Oil"):           ("wpsr.commercial_imports",         "wpsr.commercial_imports"),
    ("Crude Oil Supply", "Imports by SPR"):                 ("wpsr.imports_by_spr",             "wpsr.imports_by_spr"),
    ("Crude Oil Supply", "Imports into SPR by Others"):     ("wpsr.imports_into_spr_by_others", "wpsr.imports_into_spr_by_others"),
    ("Crude Oil Supply", "Exports"):                        ("wpsr.exports",                    "wpsr.exports"),
    ("Crude Oil Supply", "Stock Change (+/build; -/draw)"): ("wpsr.stock_change_total",         "wpsr.stock_change_total"),
    ("Crude Oil Supply", "Commercial Stock Change"):        ("wpsr.stock_change_commercial",    "wpsr.stock_change_commercial"),
    ("Crude Oil Supply", "SPR Stock Change"):               ("wpsr.stock_change_spr",           "wpsr.stock_change_spr"),
    ("Crude Oil Supply", "Adjustment"):                     ("wpsr.adjustment",                 "wpsr.adjustment"),
    ("Crude Oil Supply", "Crude Oil Input to Refineries"):  ("wpsr.crude_input_to_refineries",  "wpsr.crude_input_to_refineries"),
}

# ───────────── CSV → rows ─────────────
def extract_wpsr_file(path: Path) -> List[Tuple[str, str, datetime.date, float]]:
    rows: List[Tuple[str, str, datetime.date, float]] = []

    with path.open(encoding="utf-8", newline="") as f:
        rdr = csv.reader(f)
        section = 0
        header: List[str] = []

        for line in rdr:
            if not line:
                continue

            # detect “STUB_1” header rows
            if line[0].startswith("STUB_1"):
                section += 1
                header = line
                continue

            # section 1 – stocks
            if section == 1:
                label = line[0].strip()
                if label in STOCKS_MAP:
                    series_code, model_col = STOCKS_MAP[label]
                    obs_date = datetime.strptime(header[1], "%m/%d/%y").date()
                    value    = _num(line[1])
                    if value is not None:
                        rows.append((model_col, series_code, obs_date, value))
                continue

            # section 2 – supply / disposition
            if section == 2 and len(line) >= 3:
                key = (_clean(line[0]), _clean(line[1]))
                if key in SUPPLY_MAP:
                    series_code, model_col = SUPPLY_MAP[key]
                    obs_date = datetime.strptime(header[2], "%m/%d/%y").date()
                    value    = _num(line[2])
                    if value is not None:
                        rows.append((model_col, series_code, obs_date, value))

    return rows

# ───────────── DB helpers ─────────────
def ensure_meta(cur, series_codes: List[str]) -> None:
    cur.execute(
        f"""
        INSERT INTO {META} (series_code, source_id, description)
        SELECT s, (SELECT source_id FROM core_energy.dim_source WHERE name = %s), s
        FROM   UNNEST(%s::text[]) AS t(s)
        ON CONFLICT (series_code) DO NOTHING;
        """,
        (SOURCE_NAME, series_codes),
    )

def insert_rows(cur, rows: List[Tuple[str, str, datetime.date, float]]) -> None:
    if not rows:
        return
    ensure_meta(cur, [r[1] for r in rows])

    data: List[Tuple[int, datetime.date, float, datetime]] = []
    for _, series_code, obs_date, value in rows:
        cur.execute(f"SELECT series_id FROM {META} WHERE series_code = %s", (series_code,))
        sid = cur.fetchone()
        if sid:
            data.append((sid[0], obs_date, value, datetime.now(UTC)))

    if data:
        execute_values(
            cur,
            f"""
            INSERT INTO {TABLE} (series_id, obs_date, value, loaded_at_ts)
            VALUES %s
            ON CONFLICT (series_id, obs_date) DO NOTHING;
            """,
            data,
        )

# ───────────── Main ─────────────
def main() -> None:
    csv_files = sorted(RAW_DIR.glob("*.csv"))
    all_rows: List[Tuple[str, str, datetime.date, float]] = []

    for csv_file in csv_files:
        print(f"📄 {csv_file.name}")
        recs = extract_wpsr_file(csv_file)
        if recs:
            print(f"  ✓ {len(recs)} rows extracted")
            all_rows.extend(recs)
        else:
            print("  ⚠️  none matched mapping")

    if not all_rows:
        print("⚠️  No WPSR values parsed.")
        return

    try:
        with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
            insert_rows(cur, all_rows)
        msg = f"{len(all_rows)} observations inserted from {len(csv_files)} WPSR files."
        print("✓", msg)
        send_email(subject="WPSR loader: Success", body=msg, to=USER_EMAIL)
    except Exception as exc:
        send_email(subject="WPSR loader: FAILED", body=str(exc), to=USER_EMAIL)
        raise

if __name__ == "__main__":
    main()
