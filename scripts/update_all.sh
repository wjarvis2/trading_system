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

echo "── JODI ──"
python -m src.data_collection.jodi_collector

echo "All collectors finished."
