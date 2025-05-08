#!/usr/bin/env python
"""
src/data_collection/wpsr_collector.py
────────────────────────────────────────────────────────────────────────────
Download archived WPSR table1.csv files → data/raw/wpsr_reports/

• Tries all Wednesdays since 2011-08-03
• Downloads only if not already saved
• Stores files as table1_YYYY_MM_DD.csv
• Sends email only if new files are collected
"""

from __future__ import annotations
import os
from datetime import datetime, timedelta
from pathlib import Path
import requests

from src.utils import send_email

# ───────────────────────── paths & constants ────────────────────────── #
ROOT_DIR   = Path(__file__).resolve().parents[2]
DATA_DIR   = ROOT_DIR / "data/raw/wpsr_reports"
BASE_URL   = "https://www.eia.gov/petroleum/supply/weekly/archive"
USER_EMAIL = "jarviswilliamd@gmail.com"
START_DATE = datetime(2011, 8, 3)  # first WPSR archive with structured .csv

# ───────────────────────── helpers ────────────────────────── #
def ensure_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def list_existing_filenames() -> set[str]:
    return {f.name for f in DATA_DIR.glob("table1_*.csv")}

def attempt_download(date: datetime) -> str | None:
    ymd = date.strftime("%Y_%m_%d")
    url = f"https://www.eia.gov/petroleum/supply/weekly/archive/{date.year}/{ymd}/csv/table1.csv"
    fname = f"table1_{ymd}.csv"
    save_path = DATA_DIR / fname

    try:
        r = requests.get(url, timeout=15)
        if r.ok and len(r.text.splitlines()) > 5:  # basic sanity check
            save_path.write_text(r.text)
            print(f"✓ Downloaded {fname}")
            return fname
        else:
            print(f"✗ {fname} → valid HTTP but unexpected content or structure")
    except Exception as e:
        print(f"✗ {fname} → {e}")
    return None


# ───────────────────────── main collector ────────────────────────── #
def collect():
    ensure_dir()
    existing = list_existing_filenames()
    today = datetime.today()

    # Try each Wednesday from start date through today
    date = START_DATE
    downloaded = []
    while date <= today:
        fname = f"table1_{date.strftime('%Y_%m_%d')}.csv"
        if fname not in existing:
            result = attempt_download(date)
            if result:
                downloaded.append(result)
        date += timedelta(days=7)  # advance 1 week

    if downloaded:
        send_email(
            subject="WPSR collector: New files downloaded",
            body=f"{len(downloaded)} new table1.csv files saved:\n\n" + "\n".join(downloaded),
            to=USER_EMAIL
        )
    else:
        print("✅ No new WPSR files to download.")

if __name__ == "__main__":
    collect()
