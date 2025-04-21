# --- src/data_collection/stb_collector.py ---
import requests
import datetime as dt
from pathlib import Path
import argparse
from ..utils import send_email

OUT_DIR = Path("data/raw/stb_railcarloads_reports")
OUT_DIR.mkdir(parents=True, exist_ok=True)

RAILROADS = ["UP", "NS", "CSX", "CN", "CP", "KCS", "CTCO", "BNSF"]
USER_EMAIL = "jarviswilliamd@gmail.com"

def try_download(railroad: str, date_str: str, today_obj: dt.date) -> tuple[bool, str]:
    label = railroad.upper()
    filename = f"{label}_{today_obj.strftime('%Y%m%d')}.xlsx"
    dest = OUT_DIR / filename

    if dest.exists():
        print(f"• {label}: already exists")
        return False, f"{label} (already exists)"

    urls = [
        f"https://www.stb.gov/wp-content/uploads/files/rsir/{label}/{label}%20Data%20{date_str}.xlsx",
        f"https://www.stb.gov/wp-content/uploads/files/rsir/{label}/{label.lower()}_data_{date_str}.xlsx"
    ]

    for url in urls:
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                dest.write_bytes(r.content)
                print(f"✓ {label}: {date_str} → saved")
                return True, f"{label} (saved)"
        except Exception as e:
            print(f"⚠️ {label}: error trying {url} → {e}")

    print(f"✗ {label}: {date_str} → not found")
    return False, f"{label} (not found)"

def collect(date_override: str = None):
    today = dt.date.today() if date_override is None else dt.datetime.strptime(date_override, "%Y-%m-%d").date()
    date_str = today.strftime("%Y-%m-%d")
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M")

    print(f"\n📦 Collecting STB EP 724 weekly reports for {date_str}...\n")

    successes = []
    failures = []

    for railroad in RAILROADS:
        success, msg = try_download(railroad, date_str, today)
        if success:
            successes.append(msg)
        else:
            failures.append(msg)

    subject = "STB collector: Success" if not failures else (
        "STB collector: Partial Success" if successes else "STB collector: Failed"
    )

    body = (
        f"Run timestamp: {stamp}\n"
        f"Date requested: {date_str}\n\n"
        f"✓ Successes:\n" + ("\n".join(successes) if successes else "None") + "\n\n"
        f"✗ Failures:\n" + ("\n".join(failures) if failures else "None")
    )

    send_email(subject=subject, body=body, to=USER_EMAIL)

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--date", help="Optional date override in YYYY-MM-DD format")
        args = parser.parse_args()

        collect(date_override=args.date)

    except Exception as e:
        send_email(subject="STB collector crashed", body=str(e), to=USER_EMAIL)
        raise
