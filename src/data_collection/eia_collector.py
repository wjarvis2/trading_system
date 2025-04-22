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
CONFIG     = ROOT_DIR / "config" / "eia_series.csv"
DATA_DIR   = ROOT_DIR / "data" / "raw" / "eia_reports"
API_KEY    = os.getenv("EIA_API_KEY")
USER_EMAIL = "jarviswilliamd@gmail.com"

# ───────────────────────── helpers ────────────────────────── #
def ensure_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def read_series_list() -> List[dict]:
    with open(CONFIG, newline="") as f:
        return list(csv.DictReader(f))

def fetch_series(series_id: str, api_key: str) -> pd.DataFrame | None:
    # Try v2
    try:
        v2_url = f"https://api.eia.gov/v2/seriesid/{series_id}?api_key={api_key}"
        r = requests.get(v2_url, timeout=15)
        if r.ok:
            df = pd.DataFrame(r.json()["response"]["data"])
            if "period" in df.columns and "value" in df.columns:
                return df.rename(columns={"period": "date"})[["date", "value"]]
    except Exception as e:
        print(f"v2 error for {series_id}: {e}")

    # Fallback to v1
    try:
        v1_url = f"https://api.eia.gov/series/?api_key={api_key}&series_id={series_id}"
        r = requests.get(v1_url, timeout=15)
        if r.ok:
            return pd.DataFrame(r.json()["series"][0]["data"], columns=["date", "value"])
    except Exception as e:
        print(f"v1 error for {series_id}: {e}")

    print(f"✗ {series_id}: failed to retrieve usable data")
    return None

# ───────────────────────── main collector ────────────────────────── #
def collect() -> None:
    if not API_KEY:
        raise RuntimeError("EIA_API_KEY not set in environment")

    ensure_dir()
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M")
    daily_tag = dt.datetime.now().strftime("%Y%m%d")
    out_file = DATA_DIR / f"eia_{stamp}.csv"

    # Avoid duplicate pulls in a day
    existing_files = list(DATA_DIR.glob(f"eia_{daily_tag}_*.csv"))
    if existing_files:
        send_email(
            subject="EIA collector: No new file",
            body="Data has already been collected today.",
            to=USER_EMAIL
        )
        return

    rows = read_series_list()
    all_frames: List[pd.DataFrame] = []
    failures: List[str] = []

    for row in rows:
        sid = row["series_id"].strip()
        df = fetch_series(sid, API_KEY)
        if df is not None:
            df["series_id"] = sid
            all_frames.append(df)
            print(f"✓ {sid}")
        else:
            failures.append(sid)

    if not all_frames:
        send_email(
            subject="EIA collector: Failed",
            body="No data was collected. All series failed.",
            to=USER_EMAIL
        )
        return

    final = pd.concat(all_frames, ignore_index=True)
    final["date"] = pd.to_datetime(final["date"], errors="coerce")
    final = final.dropna(subset=["date", "value"])
    final = final.sort_values(["series_id", "date"])
    final.to_csv(out_file, index=False)

    subject = "EIA collector: Success"
    body = (
        f"Saved {len(all_frames)} series to {out_file.name}\n"
        + ("Failures: " + ", ".join(failures) if failures else "All series succeeded.")
    )
    send_email(subject=subject, body=body, to=USER_EMAIL)

if __name__ == "__main__":
    collect()
