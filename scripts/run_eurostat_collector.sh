#!/usr/bin/env bash
cd "$HOME/trading_system" || exit 1
export PYTHONPATH="$HOME/trading_system"
source venv/bin/activate
python -m src.data_collection.eurostat_collector
