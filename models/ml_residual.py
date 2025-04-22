"""
XGBoost residual layer
"""

import xgboost as xgb
import pandas as pd
from pathlib import Path

MODEL_PATH = Path("models/trained/xgb_curve.json")

def train(X: pd.DataFrame, y: pd.Series, save: bool = True):
    model = xgb.XGBRegressor(
        n_estimators=400,
        max_depth=4,
        learning_rate=0.03,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="reg:squarederror",
    )
    model.fit(X, y,
              eval_set=[(X.iloc[-200:], y.iloc[-200:])],
              early_stopping_rounds=30,
              verbose=False)
    if save:
        MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
        model.save_model(MODEL_PATH)
    return model

def load_model():
    model = xgb.XGBRegressor()
    model.load_model(MODEL_PATH)
    return model
