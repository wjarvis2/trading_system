import pandas as pd
from models.balances_core import build_balances

def test_balance_sign():
    bal = build_balances()
    # last reported EIA Δstocks should match sign of weekly print
    latest = bal.iloc[-1]
    assert latest["Δstocks_US"] * latest["stocks_US"] >= 0
