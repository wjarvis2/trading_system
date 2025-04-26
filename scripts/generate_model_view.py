import psycopg2
import pandas as pd
import os
import re
from collections import defaultdict

def safe_sql_identifier(name: str) -> str:
    name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    return name[:63]  # enforce 63-char max

# Prepare DB connection
dsn = os.getenv("PG_DSN")
if not dsn:
    raise EnvironmentError("PG_DSN is not set.")

conn = psycopg2.connect(dsn)
df = pd.read_sql("""
    SELECT model_col, series_code
    FROM core_energy.map_model_series
    ORDER BY model_col
""", conn)

# De-duplicate truncated names
used = defaultdict(int)
cols = []
for _, row in df.iterrows():
    base = safe_sql_identifier(row.model_col)
    suffix = f"_{used[base]}" if used[base] > 0 else ""
    final_name = f"{base[:63 - len(suffix)]}{suffix}"
    used[base] += 1

    cols.append(
        f"    MAX(CASE WHEN m.series_code = '{row.series_code}' THEN v.value END) AS \"{final_name}\""
    )

# Build SQL view
view_sql = """
CREATE OR REPLACE VIEW core_energy.v_model_wide AS
SELECT
    v.obs_date,
""" + ",\n".join(cols) + """
FROM core_energy.fact_series_value v
JOIN core_energy.fact_series_meta m USING (series_id)
JOIN core_energy.map_model_series map ON m.series_code = map.series_code
GROUP BY v.obs_date;
"""

# Save to file
os.makedirs("sql", exist_ok=True)
with open("sql/v_model_wide.sql", "w") as f:
    f.write(view_sql)

print("✓ SQL view script saved with deduped column names")
