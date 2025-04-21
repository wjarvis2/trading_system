# --- src/data_collection/stb_bootstrap.py ---
import requests
import datetime as dt
from pathlib import Path

OUT_DIR = Path("data/raw/stb_railcarloads_reports")
OUT_DIR.mkdir(parents=True, exist_ok=True)

RAILROADS = ["UP", "NS", "CSX", "CN", "CP", "KCS", "CTCO", "BNSF"]
START_DATE = dt.date(2016, 1, 1)
END_DATE = dt.date.today()

# ✅ Only include Wednesdays — STB filenames are based on submission date (Wednesday)
DATE_RANGE = [
    d for d in (START_DATE + dt.timedelta(days=x) for x in range((END_DATE - START_DATE).days + 1))
    if d.weekday() == 2  # 2 = Wednesday
]

def try_download(railroad: str, date: dt.date) -> bool:
    label = railroad.upper()
    date_str = date.strftime("%Y-%m-%d")
    filename = f"{label}_{date.strftime('%Y%m%d')}.xlsx"
    dest = OUT_DIR / filename

    if dest.exists():
        print(f"• {filename} already exists")
        return False

    urls = [
        f"https://www.stb.gov/wp-content/uploads/files/rsir/{label}/{label}%20Data%20{date_str}.xlsx",
        f"https://www.stb.gov/wp-content/uploads/files/rsir/{label}/{label.lower()}_data_{date_str}.xlsx"
    ]

    for url in urls:
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                dest.write_bytes(r.content)
                print(f"✓ {filename} → saved")
                return True
            else:
                print(f"✗ {filename} → {r.status_code} {url}")
        except Exception as e:
            print(f"⚠️ {filename} error: {e}")
    return False

def collect():
    print(f"Bootstrapping STB EP 724 reports from {START_DATE} to {END_DATE} (Wednesdays only)...\n")
    total = 0
    saved = 0

    for railroad in RAILROADS:
        for d in DATE_RANGE:
            total += 1
            if try_download(railroad, d):
                saved += 1

    print(f"\n📊 Done. Tried {total} files, saved {saved}, skipped {total - saved}.")

if __name__ == "__main__":
    collect()
