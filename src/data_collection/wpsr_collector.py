#!/usr/bin/env python
"""
src/data_collection/wpsr_collector.py
────────────────────────────────────────────────────────────────────────────
Download archived WPSR table1.csv files → data/raw/wpsr_reports/

v3  –  legitimacy check
• Walk every day from 2011-08-03 through today.
• Accept a download **only** if
    A) response.ok   AND
    B) "csv" in content-type header   AND
    C) first non-blank line starts with "STUB_1"
• E-mail only when new files saved.
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
USER_EMAIL = "jarviswilliamd@gmail.com"

ARCHIVE_URL = (
    "https://www.eia.gov/petroleum/supply/weekly/archive/"
    "{year}/{ymd}/csv/table1.csv"
)

START_DATE = datetime(2011, 8, 3)     # first archive entry
TIMEOUT    = 15                       # seconds

# ───────────────────────── helpers ────────────────────────── #
def ensure_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def existing_files() -> set[str]:
    return {p.name for p in DATA_DIR.glob("table1_*.csv")}

def _looks_like_csv(resp: requests.Response) -> bool:
    ctype = resp.headers.get("Content-Type", "").lower()
    if "csv" not in ctype:
        return False
    # first non-blank line
    for line in resp.text.splitlines():
        if line.strip():
            return line.startswith('"STUB_1"') or line.startswith("STUB_1")
    return False

def attempt_download(date: datetime) -> str | None:
    ymd   = date.strftime("%Y_%m_%d")
    url   = ARCHIVE_URL.format(year=date.year, ymd=ymd)
    fname = f"table1_{ymd}.csv"
    path  = DATA_DIR / fname

    try:
        resp = requests.get(url, timeout=TIMEOUT)
        if resp.ok and _looks_like_csv(resp):
            path.write_text(resp.text)
            print(f"✓ {fname}")
            return fname
        else:
            print(f"✗ {fname} — not a valid WPSR CSV")
    except Exception as exc:
        print(f"✗ {fname} — {exc}")
    return None

# ───────────────────────── main collector ────────────────────────── #
def collect() -> None:
    ensure_dir()
    have      = existing_files()
    today     = datetime.today()
    current   = START_DATE
    new_files: list[str] = []

    while current.date() <= today.date():
        fname = f"table1_{current.strftime('%Y_%m_%d')}.csv"
        if fname not in have:
            got = attempt_download(current)
            if got:
                new_files.append(got)
        current += timedelta(days=1)          # daily step

    if new_files:
        send_email(
            subject="WPSR collector: New files downloaded",
            body=f"{len(new_files)} new table1.csv files saved:\n\n" + "\n".join(new_files),
            to=USER_EMAIL,
        )
    else:
        print("✅ No new WPSR files to download.")

if __name__ == "__main__":
    collect()
