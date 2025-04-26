#!/usr/bin/env python
# --- src/data_collection/paj_collector.py ---
import datetime as dt
import requests
from pathlib import Path
from src.utils import send_email

USER_EMAIL = "jarviswilliamd@gmail.com"
OUT_DIR = Path("data/raw/paj_cruderuns_reports")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Label → (code, extension)
REPORTS = {
    "crude_sd":     ("01E", ".xlsx"),
    "product_sd":   ("02E", ".xlsx"),
    "import_price": ("03E", ".xlsx"),
    "stockpiling":  ("05E", ".xls"),
}

def build_url(code: str, ext: str, date: dt.datetime) -> str:
    month_path = date.strftime("%Y-%m")
    date_code = date.strftime("%Y%m")
    return f"https://www.paj.gr.jp/sites/default/files/{month_path}/paj-{code}_{date_code}{ext}"

def collect():
    today = dt.datetime.now()
    stamp = today.strftime("%Y%m%d")
    yyyymm = today.strftime("%Y%m")

    print(f"📦 Downloading PAJ statistical reports for {yyyymm}...")

    successes, skipped, failures = [], [], []

    for label, (code, ext) in REPORTS.items():
        fname = f"{label}_{yyyymm}{ext}"
        path = OUT_DIR / fname

        if path.exists():
            print(f"⏩ {label}: already exists ({fname})")
            skipped.append(label)
            continue

        url = build_url(code, ext, today)
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            path.write_bytes(r.content)
            print(f"✓ {label} → saved to {fname}")
            successes.append(label)
        except Exception as e:
            print(f"✗ {label} → failed: {e}")
            failures.append(label)

    # 📬 Email logic
    if successes:
        send_email(
            subject="PAJ collector: Success",
            body=(
                f"Downloaded: {', '.join(successes)}\n"
                f"Skipped: {', '.join(skipped) if skipped else 'None'}\n"
                f"Failures: {', '.join(failures) if failures else 'None'}"
            ),
            to=USER_EMAIL
        )
    elif not skipped:
        send_email(
            subject="PAJ collector: Failed",
            body="All downloads failed.",
            to=USER_EMAIL
        )
    else:
        print("All files already downloaded — no email sent.")

if __name__ == "__main__":
    collect()
