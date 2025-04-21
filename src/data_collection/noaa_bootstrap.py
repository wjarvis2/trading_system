# --- src/data_collection/noaa_bootstrap.py ---
import datetime as dt
import pandas as pd
import requests
from pathlib import Path
from io import StringIO

from src.data_collection.noaa_collector import STATIONS
from src.utils import send_email

OUT_DIR = Path("data/raw/noaa_hddcdd_reports")
OUT_DIR.mkdir(parents=True, exist_ok=True)
USER_EMAIL = "jarviswilliamd@gmail.com"
TOKEN = "zsLDDfyPJRFvzFofJCIRkCqmrXqDQyRQ"

def fetch_year_csv(station_id: str, label: str, year: int) -> pd.DataFrame | None:
    start = f"{year}-01-01"
    end = f"{year}-12-31"

    url = (
        f"https://www.ncei.noaa.gov/access/services/data/v1"
        f"?dataset=daily-summaries"
        f"&stations={station_id}"
        f"&startDate={start}"
        f"&endDate={end}"
        f"&units=standard"
        f"&format=csv"
        f"&includeStationName=true"
        f"&includeStationLocation=1"
        f"&token={TOKEN}"
    )

    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        if "STATION" not in r.text:
            return None
        df = pd.read_csv(StringIO(r.text))
        if not df.empty:
            df["station_label"] = label
            return df
    except Exception as e:
        print(f"  ✗ {label} {year} failed: {e}")
    return None

def collect_all():
    print("📦 Bootstrapping full NOAA HDD/CDD history...")
    all_dfs = []
    failures = []
    current_year = dt.datetime.now().year

    for label, station in STATIONS.items():
        for year in range(2000, current_year + 1):
            print(f"→ {label} {year}")
            df = fetch_year_csv(station, label, year)
            if df is not None:
                all_dfs.append(df)
            else:
                failures.append(f"{label} ({year})")

    if not all_dfs:
        send_email(
            subject="NOAA bootstrap: Failed",
            body="All station-year combinations failed. No data collected.",
            to=USER_EMAIL
        )
        return

    full_df = pd.concat(all_dfs, ignore_index=True)
    out_path = OUT_DIR / f"noaa_hddcdd_bootstrap_{dt.datetime.now().strftime('%Y%m%d')}.csv"
    full_df.to_csv(out_path, index=False)

    send_email(
        subject="NOAA bootstrap: Success",
        body=(
            f"Saved {len(full_df)} rows to {out_path.name}\n"
            f"Failures: {', '.join(failures) if failures else 'None'}"
        ),
        to=USER_EMAIL
    )

if __name__ == "__main__":
    collect_all()
