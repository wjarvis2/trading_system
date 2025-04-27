#!/usr/bin/env python

import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

# ────────────── Load Environment ────────────── #
load_dotenv()
conn = psycopg2.connect(os.getenv("PG_DSN"))

# ────────────── Query ────────────── #
df = pd.read_sql("""
    SELECT 
        model_col,
        series_code,
        unit,
        freq,
        description,
        COALESCE(array_to_string(tags, ','), '') AS tags
    FROM core_energy.dim_series
    ORDER BY model_col;
""", conn)

# ────────────── Output ────────────── #
output_path = "scripts/exports/model_col_context.csv"
df.to_csv(output_path, index=False)
print(f"✓ Exported to {output_path}")