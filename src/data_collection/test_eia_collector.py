#!/usr/bin/env python
from __future__ import annotations
import csv
import datetime as dt
import os
from pathlib import Path
from typing import List
import pandas as pd
import requests

from src.utils import send_email

# ───────────────────────── paths & constants ────────────────────────── #
ROOT_DIR   = Path(__file__).resolve().parents[2]
CONFIG     = ROOT_DIR / "config" / "dim_series.csv"
DATA_DIR   = ROOT_DIR / "data/raw/eia_reports_debug"
API_KEY    = os.getenv("EIA_API_KEY")
USER_EMAIL = "jarviswilliamd@gmail.com"

# ───────────────────────── helpers ────────────────────────── #
def ensure_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def read_series_list() -> List[str]:
    with open(CONFIG, newline="") as f:
        return [
            row["series_code"].strip()
            for row in csv.DictReader(f)
            if row["series_code"].strip().startswith("pet.")
        ]

def fetch_series(series_id: str, api_key: str) -> pd.DataFrame | None:
    try:
        url = f"https://api.eia.gov/v2/seriesid/{series_id}?api_key={api_key}"
        print(f"📡 Requesting {series_id}")
        r = requests.get(url, timeout=15)

        if not r.ok:
            print(f"✗ {series_id}: HTTP {r.status_code}")
            print("🔍 Response text (truncated):", r.text[:300])
            return None

        json_data = r.json()
        if "response" not in json_data or "data" not in json_data["response"]:
            print(f"✗ {series_id}: malformed JSON — missing 'response.data'")
            print("🔍 Raw JSON:", json_data)
            return None

        data = json_data["response"]["data"]
        if not data:
            print(f"✗ {series_id}: API returned no data")
            return None

        df = pd.DataFrame(data)
        if "period" not in df.columns or "value" not in df.columns:
            print(f"✗ {series_id}: missing required columns in data:", df.columns.tolist())
            return None

        return df.rename(columns={"period": "date"})[["date", "value"]]

    except Exception as e:
        print(f"✗ {series_id}: exception: {e}")
        return None

# ───────────────────────── main collector ────────────────────────── #
def collect() -> None:
    if not API_KEY:
        raise RuntimeError("EIA_API_KEY not set in environment")

    ensure_dir()
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M")
    out_file = DATA_DIR / f"eia_debug_{stamp}.csv"

    series_codes = read_series_list()
    all_frames: List[pd.DataFrame] = []
    failures: List[str] = []

    for sid_raw in series_codes:
        sid = sid_raw.upper()
        df = fetch_series(sid, API_KEY)
        if df is not None:
            df["series_id"] = sid.lower()
            all_frames.append(df)
            print(f"✓ {sid}")
        else:
            failures.append(sid)

    if not all_frames:
        print("❌ No data collected.")
        send_email(
            subject="EIA Debug Collector: All Failed",
            body="No series returned data. See terminal for detailed diagnostics.",
            to=USER_EMAIL
        )
        return

    final = pd.concat(all_frames, ignore_index=True)
    final["date"] = pd.to_datetime(final["date"], errors="coerce")
    final = final.dropna(subset=["date", "value"])
    final = final.sort_values(["series_id", "date"])
    final.to_csv(out_file, index=False)

    print(f"✅ Saved {len(all_frames)} series to {out_file.name}")
    if failures:
        print("⚠️ Failures:", ", ".join(failures))

if __name__ == "__main__":
    collect()
