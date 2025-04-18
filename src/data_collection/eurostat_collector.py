from __future__ import annotations
"""Eurostat data collector

• Reads table list from config/eurostat_tables.csv (table_id,url,description)
• Downloads each URL twice‑daily (11:00 and 23:00 CET ≈ 05:00 / 17:00 ET)
• Saves raw files in data/raw/eurostat_reports/<table_id>_<YYYYMMDD_HHMM>.csv
• E‑mails user on success / failure via utils.send_email

Usage
-----
Run once (incremental):
    python -m src.data_collection.eurostat_collector

Schedule via cron at e.g. 05:10 and 17:10 Eastern:
    10 5 * * * /home/will/trading_system/scripts/run_eurostat_collector.sh
    10 17 * * * /home/will/trading_system/scripts/run_eurostat_collector.sh
"""

import csv
import datetime as dt
import os
import re
import sys
from pathlib import Path
from typing import Optional

import requests

# local utils
from src.utils import send_email


# ---------------------------------------------------------------------
# paths & constants
# ---------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]  # trading_system/
CFG  = ROOT / "config" / "eurostat_table_links.csv"
DATA_DIR = ROOT / "data" / "raw" / "eurostat_reports"
USER_EMAIL = "jarviswilliamd@gmail.com"

CET = dt.timezone(dt.timedelta(hours=1))  # Central European Time (no DST logic needed for daily runs)
EST = dt.timezone(dt.timedelta(hours=-5))

# ---------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------

def ensure_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def read_table_list() -> list[dict[str, str]]:
    if not CFG.exists():
        raise FileNotFoundError(f"Table list missing: {CFG}")
    with CFG.open(newline="") as f:
        reader = csv.DictReader(f)
        rows = [r for r in reader if r.get("url")]
    return rows


def download(url: str) -> Optional[bytes]:
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            return resp.content
    except Exception:
        pass
    return None


def save_file(table_id: str, content: bytes, ext: str = "csv") -> Path:
    ensure_dir()
    ts = dt.datetime.now(EST).strftime("%Y%m%d_%H%M")
    fname = f"{table_id}_{ts}.{ext}"
    path = DATA_DIR / fname
    path.write_bytes(content)
    return path


def collect() -> None:
    rows = read_table_list()
    successes: list[str] = []
    failures: list[str] = []

    for row in rows:
        tid = row["table_id"].strip()
        url = row["url"].strip()
        if not tid or not url:
            continue

        print(f"→ {tid}")
        content = download(url)
        if content:
            # infer extension from URL (csv/json/tsv)
            ext_match = re.search(r"\.(csv|tsv|json)(?:\?|$)", url)
            ext = ext_match.group(1) if ext_match else "csv"
            path = save_file(tid, content, ext)
            print(f"  ✓ saved {path.name}")
            successes.append(tid)
        else:
            print(f"  ✗ failed {tid}")
            failures.append(tid)

    # e‑mail summary
    subj = "Eurostat collector: success" if not failures else "Eurostat collector: some failures"
    body_lines = [f"Run at {dt.datetime.now(EST).strftime('%Y-%m-%d %H:%M %Z')}", ""]
    if successes:
        body_lines.append(f"Downloaded: {', '.join(successes)}")
    if failures:
        body_lines.append(f"Failed: {', '.join(failures)}")
    send_email(subj, "\n".join(body_lines), USER_EMAIL)


if __name__ == "__main__":
    try:
        collect()
    except Exception as exc:
        # send fatal error email
        send_email("Eurostat collector crashed", str(exc), USER_EMAIL)
        raise
