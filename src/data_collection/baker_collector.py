# --- src/data_collection/baker_collector.py ---
import datetime as dt
from pathlib import Path
import requests
from bs4 import BeautifulSoup

from src.utils import send_email

# ────────────── Constants ────────────── #
PAGE_URL      = "https://rigcount.bakerhughes.com/na-rig-count"
STATIC_PREFIX = "https://rigcount.bakerhughes.com"
OUT_DIR       = Path("data/raw/bh_rigcount_reports")
USER_EMAIL    = "jarviswilliamd@gmail.com"
TS            = dt.datetime.now().strftime("%Y%m%d")
OUT_FILE      = OUT_DIR / f"bh_rigcount_{TS}.xlsx"

# ────────────── Setup ────────────── #
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ────────────── Scraper ────────────── #
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

# ────────────── Main Collector ────────────── #
def collect():
    try:
        if OUT_FILE.exists():
            print(f"⏩ Rig count file already exists: {OUT_FILE.name}")
            return  # ✅ No email for already-downloaded files

        print("🔍 Looking for latest Baker Hughes rig count XLSX...")
        url = find_latest_xlsx_link()
        print(f"→ Found: {url}")

        r = requests.get(url)
        r.raise_for_status()
        OUT_FILE.write_bytes(r.content)

        print(f"✓ Saved rig count file to: {OUT_FILE.name}")
        send_email(
            subject="Baker Hughes collector: Success",
            body=f"Saved rig count file to {OUT_FILE.name}",
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
