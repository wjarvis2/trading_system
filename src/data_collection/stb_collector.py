#!/usr/bin/env python
# --- src/data_collection/stb_collector.py ---
import requests
import datetime as dt
from pathlib import Path
import argparse
from src.utils import send_email

OUT_DIR = Path("data/raw/stb_railcarloads_reports")
OUT_DIR.mkdir(parents=True, exist_ok=True)

RAILROADS = ["UP", "NS", "CSX", "CN", "CP", "KCS", "CTCO", "BNSF"]
USER_EMAIL = "jarviswilliamd@gmail.com"

def try_download(railroad: str, file_date: dt.date) -> tuple[bool, str]:
    label = railroad.upper()
    file_stamp = file_date.strftime("%Y%m%d")
    file_slug = file_date.strftime("%Y-%m-%d")
    filename = f"{label}_{file_stamp}.xlsx"
    dest = OUT_DIR / filename

    if dest.exists():
        return False, f"{label} ({file_stamp}) already exists"

    urls = [
        f"https://www.stb.gov/wp-content/uploads/files/rsir/{label}/{label}%20Data%20{file_slug}.xlsx",
        f"https://www.stb.gov/wp-content/uploads/files/rsir/{label}/{label.lower()}_data_{file_slug}.xlsx"
    ]

    for url in urls:
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                dest.write_bytes(r.content)
                return True, f"{label} ({file_stamp}) saved"
        except Exception as e:
            print(f"⚠️ {label}: error trying {url} → {e}")

    return False, f"{label} ({file_stamp}) not found"

def collect(date_override: str = None):
    today = dt.date.today() if date_override is None else dt.datetime.strptime(date_override, "%Y-%m-%d").date()
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M")

    print(f"\n📦 Collecting STB EP 724 weekly reports around {today}...\n")

    successes, skipped, failures = [], [], []

    for offset in range(-2, 3):  # today -2 through today +2
        check_date = today + dt.timedelta(days=offset)
        for railroad in RAILROADS:
            success, msg = try_download(railroad, check_date)
            if "already exists" in msg:
                skipped.append(msg)
            elif success:
                successes.append(msg)
            else:
                failures.append(msg)

    # Email conditions
    if successes:
        subject = "STB collector: Success"
    elif failures and not skipped:
        subject = "STB collector: Failed"
    elif failures and skipped:
        subject = "STB collector: Partial Success"
    else:
        print("All reports already downloaded or not posted — no email sent.")
        return

    body = (
        f"Run timestamp: {stamp}\n"
        f"Scan window: {today - dt.timedelta(days=2)} to {today + dt.timedelta(days=2)}\n\n"
        f"✓ Successes:\n" + ("\n".join(successes) if successes else "None") + "\n\n"
        f"⏩ Skipped:\n"   + ("\n".join(skipped) if skipped else "None") + "\n\n"
        f"✗ Failures:\n"  + ("\n".join(failures) if failures else "None")
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
