from __future__ import annotations
"""
EIA data collector

Reads series IDs from config/eia_series.csv, fetches each series via the
EIA open‑data API, and stores a tidy snapshot CSV in
    data/raw/eia_reports/eia_<YYYYMMDD_HHMM>.csv

Usage
-----
Incremental run (default):
    python -m src.data_collection.eia_collector
"""

import csv
import datetime as dt
import os
from pathlib import Path
from typing import List

import pandas as pd
import requests

from ..utils import send_email

# ───────────────────────── paths & constants ────────────────────────── #
ROOT_DIR = next(p for p in Path(__file__).resolve().parents if p.name == "trading_system")
CONFIG   = ROOT_DIR / "config" / "eia_series.csv"
DATA_DIR = ROOT_DIR / "data" / "raw" / "eia_reports"

API_KEY  = os.getenv("EIA_API_KEY")      # set in .env
USER_EMAIL = "jarviswilliamd@gmail.com"

# ───────────────────────── helpers ────────────────────────── #
def ensure_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def read_series_list() -> List[dict]:
    with open(CONFIG, newline="") as f:
        return list(csv.DictReader(f))


def fetch_series(series_id: str, api_key: str) -> pd.DataFrame | None:
    """Fetch a series via v2, fall back to v1. Return DataFrame or None."""
    # v2 endpoint
    v2_url = f"https://api.eia.gov/v2/seriesid/{series_id}?api_key={api_key}"
    r = requests.get(v2_url, timeout=15)
    if r.ok:
        return pd.DataFrame(r.json()["response"]["data"])

    # legacy v1 endpoint
    v1_url = f"https://api.eia.gov/series/?api_key={api_key}&series_id={series_id}"
    r = requests.get(v1_url, timeout=15)
    if r.ok:
        # v1 returns list of [date, value]; give columns names
        return pd.DataFrame(r.json()["series"][0]["data"], columns=["date", "value"])

    print(f"✗ {series_id}: {r.status_code} (v2 & v1 failed)")
    return None


# ───────────────────────── main collector ────────────────────────── #
def collect() -> None:
    if not API_KEY:
        raise RuntimeError("EIA_API_KEY not set in environment or .env file")

    ensure_dir()
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M")
    out_file = DATA_DIR / f"eia_{stamp}.csv"

    rows = read_series_list()
    all_frames: List[pd.DataFrame] = []
    failures: List[str] = []

    for row in rows:
        sid = row["series_id"].strip()
        df = fetch_series(sid, API_KEY)
        if df is not None:
            df["series_id"] = sid        # tag each row for downstream joins
            all_frames.append(df)
            print(f"✓ {sid}")
        else:
            failures.append(sid)

    if all_frames:
        pd.concat(all_frames, ignore_index=True).to_csv(out_file, index=False)

    # email summary
    subject = "EIA collector: Success"
    body = (
        f"Saved {len(all_frames)} series to {out_file}\n" +
        ("Failures: " + ", ".join(failures) if failures else "All series succeeded.")
    )
    send_email(subject, body, USER_EMAIL)


if __name__ == "__main__":
    collect()
