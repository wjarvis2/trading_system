#!/usr/bin/env python
"""
Load (or refresh) dim_series, map_model_series, dim_series_alias, and v_series_wide
────────────────────────────────────────────────────────────────────────────────────
• Reads config/dim_series.csv  ← includes optional "aliases" column
• Bulk-upserts dim_series & map_model_series
• Inserts every non-blank alias into core_energy.dim_series_alias
• Auto-adds google_mobility headline aliases
• Rebuilds v_series_wide directly from dim_series (no manual SQL edits)

Run:
    $ python scripts/load_dim_series_from_csv.py        # uses PG_DSN in .env
"""
from __future__ import annotations

import json, os
from pathlib import Path
from typing import List

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ───────────── Config ─────────────
ROOT = Path(__file__).resolve().parents[1]
CSV  = ROOT / "config" / "dim_series.csv"

load_dotenv()
PG_DSN = os.getenv("PG_DSN")
assert PG_DSN, "PG_DSN missing from .env"

# ───────────── Read & validate ─────────────
df = pd.read_csv(CSV)

REQUIRED = {"model_col", "series_code", "unit", "freq", "description", "tags"}
missing = REQUIRED - set(df.columns)
if missing:
    raise ValueError(f"CSV missing column(s): {', '.join(sorted(missing))}")

# optional aliases column
if "aliases" not in df.columns:
    df["aliases"] = ""

# uniqueness checks
if not df["model_col"].is_unique:
    dupes = df["model_col"][df["model_col"].duplicated()].tolist()
    raise ValueError(f"Duplicate model_col values: {dupes}")
if not df["series_code"].is_unique:
    dupes = df["series_code"][df["series_code"].duplicated()].tolist()
    raise ValueError(f"Duplicate series_code values: {dupes}")

# ───────────── Tidy tags column ─────────────
def to_pg_array(cell) -> str:
    if pd.isna(cell) or cell == "":
        return "{}"
    s = str(cell).strip()
    if s.startswith("{") and s.endswith("}"):
        return s
    try:
        js = json.loads(s)
        if isinstance(js, list):
            return "{" + ",".join(js) + "}"
    except json.JSONDecodeError:
        pass
    return "{" + ",".join(p.strip() for p in s.split(",") if p.strip()) + "}"

df["tags"] = df["tags"].apply(to_pg_array)

for col in ["model_col", "series_code", "unit", "freq"]:
    df[col] = df[col].astype(str).str.strip().str.lower()

# ───────────── Insert dim_series & map_model_series ─────────────
UPSERT_DIM = """
INSERT INTO core_energy.dim_series
      (model_col, series_code, unit, freq, description, tags)
SELECT  UNNEST(:model_col)
      , UNNEST(:series_code)
      , UNNEST(:unit)
      , UNNEST(:freq)
      , UNNEST(:description)
      , UNNEST(:tags)::text[]
ON CONFLICT (model_col) DO UPDATE SET
      series_code = EXCLUDED.series_code,
      unit        = EXCLUDED.unit,
      freq        = EXCLUDED.freq,
      description = EXCLUDED.description,
      tags        = EXCLUDED.tags;
"""

UPSERT_MAP = """
INSERT INTO core_energy.map_model_series (model_col, series_code)
SELECT UNNEST(:model_col), UNNEST(:series_code)
ON CONFLICT (model_col) DO UPDATE SET
      series_code = EXCLUDED.series_code;
"""

engine = create_engine(PG_DSN)

with engine.begin() as conn:
    # ① dim_series + map_model_series
    params = {c: df[c].tolist() for c in ["model_col", "series_code", "unit", "freq", "description", "tags"]}
    conn.execute(text(UPSERT_DIM), params)
    conn.execute(text(UPSERT_MAP), params)

    # ② dim_series_alias — from CSV "aliases" column
    alias_df = (
        df.assign(aliases=df["aliases"].fillna("").astype(str).str.strip())
          .query("aliases != ''")
          .copy()
    )
    if not alias_df.empty:
        alias_df["aliases"] = alias_df["aliases"].str.split(",")
        alias_df = alias_df.explode("aliases")
        alias_params = {
            "alias_code": alias_df["aliases"].str.strip().tolist(),
            "series_code": alias_df["series_code"].tolist(),
        }
        conn.execute(text("""
            INSERT INTO core_energy.dim_series_alias (alias_code, series_code)
            SELECT UNNEST(:alias_code), UNNEST(:series_code)
            ON CONFLICT DO NOTHING;
        """), alias_params)

    # ③ dim_series_alias — add Google Mobility US headlines
    conn.execute(text("""
        INSERT INTO core_energy.dim_series_alias (alias_code, series_code)
        SELECT
            CASE split_part(series_code, '.', 3)
                WHEN 'grocery_pharmacy' THEN 'google_mobility.united_states.grocery'
                WHEN 'parks' THEN 'google_mobility.united_states.parks'
                WHEN 'residential' THEN 'google_mobility.united_states.residential'
                WHEN 'retail_recreation' THEN 'google_mobility.united_states.retail'
                WHEN 'transit_stations' THEN 'google_mobility.united_states.transit'
                WHEN 'workplaces' THEN 'google_mobility.united_states.work'
            END,
            series_code
        FROM core_energy.dim_series
        WHERE series_code LIKE 'google.us.%'
          AND split_part(series_code, '.', 3) IN (
              'grocery_pharmacy', 'parks', 'residential',
              'retail_recreation', 'transit_stations', 'workplaces'
          )
ON CONFLICT DO NOTHING;
    """))
    print("✅ dim_series_alias: Google Mobility shortcuts refreshed.")

    # ④ rebuild v_series_wide
    cols: List[str] = conn.execute(
        text("SELECT model_col FROM core_energy.dim_series ORDER BY model_col")
    ).scalars().all()

    select_lines = ",\n".join(
        f"    MAX(value) FILTER (WHERE d.model_col = '{c}') AS {c}" for c in cols
    )

    ddl = f"""
    CREATE OR REPLACE VIEW core_energy.v_series_wide AS
    SELECT
        v.obs_date::date AS date,
{select_lines}
    FROM   core_energy.fact_series_value v
    JOIN   core_energy.fact_series_meta  m USING (series_id)
    JOIN   core_energy.dim_series        d ON d.series_code = m.series_code
    GROUP  BY v.obs_date
    ORDER  BY v.obs_date;
    """

    conn.execute(text("DROP VIEW IF EXISTS core_energy.v_series_wide CASCADE"))
    conn.execute(text(ddl))

print("✅ dim_series, map_model_series, dim_series_alias (including Google Mobility), and v_series_wide fully refreshed.")
