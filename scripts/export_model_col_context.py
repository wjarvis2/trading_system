import pandas as pd
import psycopg2
import os

conn = psycopg2.connect(os.getenv("PG_DSN"))

df = pd.read_sql("""
    SELECT 
        map.model_col, 
        meta.series_code, 
        COALESCE(meta.description, '') AS description
    FROM core_energy.map_model_series map
    JOIN core_energy.fact_series_meta meta
      ON map.series_code = meta.series_code
    ORDER BY model_col;
""", conn)

df.to_csv("exports/model_col_context.csv", index=False)
print("✓ Exported to exports/model_col_context.csv")
