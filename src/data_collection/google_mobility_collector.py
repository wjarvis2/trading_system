# --- src/data_collection/google_mobility_collector.py ---
import datetime as dt
from pathlib import Path
import requests
from ..utils import send_email

OUT_DIR = Path("data/raw/google_mobility_reports")
OUT_DIR.mkdir(parents=True, exist_ok=True)

URL = "https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv"
USER_EMAIL = "jarviswilliamd@gmail.com"

def collect():
    ts = dt.datetime.now().strftime("%Y%m%d")
    file_path = OUT_DIR / f"google_mobility_{ts}.csv"

    try:
        if file_path.exists():
            print(f"• File already exists: {file_path.name}")
            send_email(
                subject="Google Mobility collector: Skipped",
                body=f"{file_path.name} already exists — skipping download.",
                to=USER_EMAIL
            )
            return

        print(f"→ Downloading Google Mobility report...")
        r = requests.get(URL, timeout=60)
        r.raise_for_status()

        file_path.write_bytes(r.content)
        size_mb = round(len(r.content) / 1_000_000, 1)
        print(f"✓ Saved {file_path.name} ({size_mb} MB)")

        send_email(
            subject="Google Mobility collector: Success",
            body=f"Saved {file_path.name} ({size_mb} MB)",
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
