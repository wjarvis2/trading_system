# --- src/data_collection/baker_collector.py ---
import datetime as dt
from pathlib import Path
import requests
from bs4 import BeautifulSoup

from ..utils import send_email

# Page with the XLSX file link
PAGE_URL = "https://rigcount.bakerhughes.com/na-rig-count"
STATIC_PREFIX = "https://rigcount.bakerhughes.com"

OUT_DIR = Path("data/raw/bh_rigcount_reports")
OUT_DIR.mkdir(parents=True, exist_ok=True)

TS = dt.datetime.now().strftime("%Y%m%d")
USER_EMAIL = "jarviswilliamd@gmail.com"

def find_latest_xlsx_link():
    res = requests.get(PAGE_URL)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")

    # Preferred MIME-type match
    for a in soup.find_all("a", href=True):
        if a.get("type") == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
            href = a["href"]
            return STATIC_PREFIX + href if href.startswith("/static-files/") else href

    # Fallback: .xlsx extension
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".xlsx" in href:
            return STATIC_PREFIX + href if href.startswith("/static-files/") else href

    raise Exception("❌ Could not find valid XLSX file on Baker Hughes rig count page.")

def collect():
    try:
        print("🔍 Looking for latest Baker Hughes rig count XLSX...")
        url = find_latest_xlsx_link()
        print(f"→ Found: {url}")

        file_path = OUT_DIR / f"bh_rigcount_{TS}.xlsx"
        r = requests.get(url)
        r.raise_for_status()
        file_path.write_bytes(r.content)

        print(f"✓ Saved rig count file to: {file_path.name}")
        send_email(
            subject="Baker Hughes collector: Success",
            body=f"Saved rig count file to {file_path.name}",
            to=USER_EMAIL
        )

    except Exception as e:
        print(f"✗ Error: {e}")
        send_email(
            subject="Baker Hughes collector: Failed",
            body=f"Error: {str(e)}",
            to=USER_EMAIL
        )

if __name__ == "__main__":
    collect()
