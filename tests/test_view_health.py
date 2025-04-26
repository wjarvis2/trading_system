import os
import pytest
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

@pytest.fixture(scope="module")
def engine():
    return create_engine(os.getenv("PG_DSN"))

def test_v_series_wide_exists(engine):
    """Ensure v_series_wide has at least one row and one data column."""
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM core_energy.v_series_wide LIMIT 5", conn)
    
    assert not df.empty, "❌ v_series_wide view is empty!"
    assert "date" in df.columns, "❌ v_series_wide missing 'date' column!"
    assert len(df.columns) > 1, "❌ v_series_wide has no data columns!"
