#!/usr/bin/env bash
# Run every incremental data‑collector once.

set -e
cd "$HOME/trading_system" || exit 1
export PYTHONPATH="$HOME/trading_system"
source venv/bin/activate

echo "── OPEC ──"
python -m src.data_collection.opec_collector

echo "── Eurostat ──"
python -m src.data_collection.eurostat_collector

echo "── EIA ──"
python -m src.data_collection.eia_collector

echo "── STB ──"
python -m src.data_collection.stb_collector

echo "── Baker Hughes ──"
python -m src.data_collection.baker_collector

echo "── Google Mobility ──"
python -m src.data_collection.google_mobility_collector

echo "── CLIMDIV ──"
python -m src.data_collection.climdiv_collector

echo "── PAJ ──"
python -m src.data_collection.paj_collector

echo "All collectors finished."
