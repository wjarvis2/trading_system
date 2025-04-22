"""
Spot‑to‑forward fair‑value calculator
"""

import numpy as np
import pandas as pd
from typing import Sequence

def fair_curve(spot: float,
               rates: pd.Series,
               storage: float,
               a: float,
               b: float,
               inv_pct: float,
               expiries: Sequence[pd.Timestamp],
               today: pd.Timestamp) -> pd.DataFrame:
    y = a - b * inv_pct          # convenience yield
    out = []
    for T in expiries:
        tau = (T - today).days / 365
        fwd = spot * np.exp((rates.loc[T] + storage - y) * tau)
        out.append({"expiry": T, "fair_fwd": fwd})
    return pd.DataFrame(out)
