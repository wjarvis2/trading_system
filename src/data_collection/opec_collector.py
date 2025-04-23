# --- src/data_collection/opec_collector.py ---
from __future__ import annotations
import csv
import datetime as dt
import os
import time
from pathlib import Path
from typing import Optional, List
import requests

from src.utils import send_email

# ────────────── Paths & Constants ────────────── #
ROOT_DIR   = Path(__file__).resolve().parents[2]
DATA_DIR   = ROOT_DIR / "data" / "raw" / "opec_reports"
CONF_FILE  = ROOT_DIR / "config" / "opec_schedule_2025.csv"
USER_EMAIL = "jarviswilliamd@gmail.com"

TZ_EST = dt.timezone(dt.timedelta(hours=-5))  # No DST
NEW_FMT = "https://www.opec.org/assets/assetdb/momr-appendix-{month}-{year}.xlsx"
OLD_FMT = "https://www.opec.org/assets/assetdb/appendix-tables-{month}-{year}.xlsx"
MONTH_LC = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]

# ────────────── Helpers ────────────── #
def ensure_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def url_candidates(month: int, year: int) -> List[str]:
    m = MONTH_LC[month - 1]
    return [NEW_FMT.format(month=m, year=year), OLD_FMT.format(month=m, year=year)]

def download(url: str, dest: Path) -> bool:
    try:
        r = requests.get(url, timeout=20)
        if r.status_code == 200 and r.content[:2] == b"PK":
            dest.write_bytes(r.content)
            return True
    except Exception:
        pass
    return False

def load_schedule() -> set[dt.date]:
    with open(CONF_FILE, newline="") as f:
        return {
            dt.datetime.strptime(row["release_date"], "%Y-%m-%d").date()
            for row in csv.DictReader(f)
        }

# ────────────── Backfill ────────────── #
def backfill() -> None:
    ensure_dir()
    start = dt.date(2019, 1, 1)
    today = dt.date.today()

    for year in range(start.year, today.year + 1):
        for month in range(1, 13):
            d = dt.date(year, month, 1)
            if d > today:
                break
            fn = DATA_DIR / f"opec_{year}-{month:02}.xlsx"
            if fn.exists():
                continue
            for url in url_candidates(month, year):
                if download(url, fn):
                    print(f"✓ {fn.name}")
                    break

# ────────────── Daily Collector ────────────── #
def run_daily_check(now: Optional[dt.datetime] = None) -> None:
    ensure_dir()
    now   = now or dt.datetime.now(tz=TZ_EST)
    today = now.date()
    schedule = load_schedule()

    if today not in schedule:
        return  # ✅ No email if today is not a scheduled release date

    outfile = DATA_DIR / f"opec_{today.year}-{today.month:02}.xlsx"
    if outfile.exists():
        print(f"⏩ OPEC report already exists: {outfile.name}")
        return  # ✅ No email if already downloaded

    deadline = dt.datetime.combine(today, dt.time(6, 5), tzinfo=TZ_EST)
    success = False

    while dt.datetime.now(tz=TZ_EST) <= deadline and not success:
        for url in url_candidates(today.month, today.year):
            success = download(url, outfile)
            if success:
                print(f"✓ OPEC report downloaded: {outfile.name}")
                send_email(
                    subject=f"OPEC report {today} downloaded: Success",
                    body=f"Saved to {outfile}",
                    to=USER_EMAIL,
                )
                break
        if not success:
            time.sleep(30)

    if not success:
        send_email(
            subject=f"OPEC report {today} FAILED",
            body="Could not download the scheduled OPEC report by 06:05 EST. Please investigate.",
            to=USER_EMAIL,
        )

    if today == max(schedule):
        send_email(
            subject="OPEC schedule exhausted",
            body="OPEC release schedule reached its final date. Please update opec_schedule_*.csv.",
            to=USER_EMAIL,
        )

# ────────────── CLI ────────────── #
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="OPEC MOMR appendix collector.")
    parser.add_argument("--backfill", action="store_true", help="Download all historical reports.")
    args = parser.parse_args()

    if args.backfill:
        backfill()
    else:
        run_daily_check()
