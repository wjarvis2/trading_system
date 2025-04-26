#!/usr/bin/env python
import os
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("EIA_API_KEY")  # or "EIA_TOKEN" if you use that

BASE_URL = "https://api.eia.gov/v2/seriesid/"
CSV_PATH = "config/eia_series.csv"

df = pd.read_csv(CSV_PATH, dtype=str)
series_ids = df["series_id"].dropna().str.strip().str.upper().tolist()

print("🔍 Validating EIA series via API v2...\n")

for sid in series_ids:
    url = f"{BASE_URL}{sid}"
    params = {"api_key": API_KEY}
    try:
        resp = requests.get(url, params=params)
        data = resp.json()

        if "response" in data and "data" in data["response"] and data["response"]["data"]:
            print(f"✅ {sid}")
        else:
            print(f"❌ {sid} - Not found or empty")
    except Exception as e:
        print(f"⚠️  {sid} - ERROR: {e}")
