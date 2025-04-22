# --- src/data_collection/google_mobility_collector.py ---
import datetime as dt
from pathlib import Path
import requests

from src.utils import send_email

OUT_DIR     = Path("data/raw/google_mobility_reports")
URL         = "https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv"
USER_EMAIL  = "jarviswilliamd@gmail.com"
TS          = dt.datetime.now().strftime("%Y%m%d")
OUT_FILE    = OUT_DIR / f"google_mobility_{TS}.csv"

OUT_DIR.mkdir(parents=True, exist_ok=True)

def collect():
    try:
        print("→ Downloading Google Mobility report...")
        r = requests.get(URL, timeout=60)
        r.raise_for_status()

        # Always overwrite if exists (per your logic)
        if OUT_FILE.exists():
            OUT_FILE.unlink()

        OUT_FILE.write_bytes(r.content)
        size_mb = round(len(r.content) / 1_000_000, 1)
        print(f"✓ Saved {OUT_FILE.name} ({size_mb} MB)")

        send_email(
            subject="Google Mobility collector: Success",
            body=f"Saved {OUT_FILE.name} ({size_mb} MB)",
            to=USER_EMAIL
        )

    except Exception as e:
        print(f"✗ Error: {e}")
        send_email(
            subject="Google Mobility collector: Failed",
            body=f"Error: {str(e)}",
            to=USER_EMAIL
        )

if __name__ == "__main__":
    collect()
