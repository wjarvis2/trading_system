# --- src/data_collection/noaa_collector.py ---
import os
import requests
import pandas as pd
import datetime as dt
from pathlib import Path
from dotenv import load_dotenv
from ..utils import send_email

load_dotenv()

OUT_DIR = Path("data/raw/noaa_hddcdd_reports")
OUT_DIR.mkdir(parents=True, exist_ok=True)

TOKEN = os.environ["NOAA_TOKEN"]
USER_EMAIL = "jarviswilliamd@gmail.com"

# Key representative stations and regions (HDD/CDD relevant for heating oil + crude)
STATIONS = {
    "NYC": "GHCND:USW00094728",        # Central Park, NY (PADD 1A)
    "Boston": "GHCND:USW00014739",     # Logan Intl, MA (PADD 1A)
    "Philly": "GHCND:USW00013739",     # Philadelphia, PA (PADD 1B)
    "Chicago": "GHCND:USW00094846",    # Chicago, IL (PADD 2)
    "Houston": "GHCND:USW00012918",    # Houston, TX (PADD 3)
    "Atlanta": "GHCND:USW00013874",    # Atlanta, GA (PADD 1C)
    "Denver": "GHCND:USW00003017",     # Denver, CO
    "Seattle": "GHCND:USW00024233",    # Seattle, WA (PADD 5)
    "LosAngeles": "GHCND:USW00023174", # LAX, CA (PADD 5)
    "CONUS": "GHCND:USW00094846"       # Placeholder for national proxy
}

def collect(start_date: str = None, end_date: str = None):
    if start_date is None:
        start_date = (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    if end_date is None:
        end_date = start_date

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M")
    out_file = OUT_DIR / f"noaa_hddcdd_{ts}.csv"
    all_dfs = []
    failures = []

    try:
        for label, station in STATIONS.items():
            print(f"→ Fetching NOAA data for {label} ({station})")
            url = (
                f"https://www.ncei.noaa.gov/access/services/data/v1"
                f"?dataset=daily-summaries"
                f"&stations={station}"
                f"&startDate={start_date}"
                f"&endDate={end_date}"
                f"&units=standard"
                f"&format=json"
                f"&includeAttributes=false"
                f"&token={TOKEN}"
            )
            try:
                r = requests.get(url, timeout=30)
                r.raise_for_status()
                data = r.json()
                df = pd.DataFrame(data)
                if not df.empty:
                    df["station_label"] = label
                    all_dfs.append(df)
                else:
                    failures.append(label + " (empty)")
            except Exception as e:
                print(f"  ✗ {label} failed: {e}")
                failures.append(f"{label} ({e})")

        if not all_dfs:
            raise RuntimeError("All station downloads failed — no data collected.")

        full_df = pd.concat(all_dfs, ignore_index=True)
        full_df.to_csv(out_file, index=False)
        print(f"✓ NOAA HDD/CDD snapshot saved with {len(full_df)} rows.")

        send_email(
            subject="NOAA collector: Success",
            body=f"Saved {len(full_df)} rows to {out_file.name}\nFailed stations: {', '.join(failures) if failures else 'None'}",
            to=USER_EMAIL
        )

    except Exception as e:
        print(f"✗ Collector failed: {e}")
        send_email(
            subject="NOAA collector: Failed",
            body=f"Error: {str(e)}",
            to=USER_EMAIL
        )

if __name__ == "__main__":
    collect()
