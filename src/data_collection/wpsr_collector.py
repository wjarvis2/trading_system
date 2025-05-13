#!/usr/bin/env python
"""
src/data_collection/wpsr_collector.py  – v4
────────────────────────────────────────────────────────────────────────────
Download archived WPSR table1.csv files → data/raw/wpsr_reports/

Changes v3 → v4
• Scan window begins **one day after the latest file we already hold**,
  instead of hard-starting at 2011-08-03 every run.
• Daily step retained → still picks up holiday-shifted releases.
• Legitimacy checks unchanged.
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

START_DATE = datetime(2011, 8, 3)   # historical floor (never re-scanned after v4)
TIMEOUT    = 15                     # seconds

# ───────────────────────── helpers ────────────────────────── #
def ensure_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def existing_files() -> dict[str, datetime]:
    """return {filename -> parsed date} for table1_YYYY_MM_DD.csv already present"""
    files = {}
    for p in DATA_DIR.glob("table1_*.csv"):
        try:
            y, m, d = p.stem.split("_")[1:]
            files[p.name] = datetime(int(y), int(m), int(d))
        except Exception:
            continue
    return files

def _looks_like_csv(resp: requests.Response) -> bool:
    ctype = resp.headers.get("Content-Type", "").lower()
    if "csv" not in ctype:
        return False
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
        r = requests.get(url, timeout=TIMEOUT)
        if r.ok and _looks_like_csv(r):
            path.write_text(r.text)
            print(f"✓ {fname}")
            return fname
        print(f"✗ {fname} — not a valid WPSR CSV")
    except Exception as exc:
        print(f"✗ {fname} — {exc}")
    return None

# ───────────────────────── main collector ────────────────────────── #
def collect() -> None:
    ensure_dir()
    existing  = existing_files()
    new_files: list[str] = []

    # determine scan window
    if existing:
        last_date = max(existing.values())
        current   = last_date + timedelta(days=1)
    else:
        current   = START_DATE

    today = datetime.today()

    print(f"Scanning from {current.date()} → {today.date()}")

    while current.date() <= today.date():
        fname = f"table1_{current.strftime('%Y_%m_%d')}.csv"
        if fname not in existing:
            got = attempt_download(current)
            if got:
                new_files.append(got)
        current += timedelta(days=1)

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
