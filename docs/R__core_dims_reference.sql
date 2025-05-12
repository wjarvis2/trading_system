/*  R__core_dims_reference.sql
    ------------------------------------------------------------------
    Reference DDL for the core_energy “bronze” layer.
    Runs on blank databases only (repeatable migration).
    All objects created with IF NOT EXISTS so it is harmless
    when executed on populated environments.
------------------------------------------------------------------- */

CREATE SCHEMA IF NOT EXISTS core_energy;

CREATE TABLE IF NOT EXISTS core_energy.dim_source (
    source_id      SERIAL PRIMARY KEY,
    name           TEXT UNIQUE NOT NULL,
    url            TEXT,
    default_freq   TEXT
);

CREATE TABLE IF NOT EXISTS core_energy.fact_series_meta (
    series_id    SERIAL PRIMARY KEY,
    series_code  TEXT UNIQUE,
    source_id    INT REFERENCES core_energy.dim_source,
    description  TEXT
);

CREATE TABLE IF NOT EXISTS core_energy.fact_series_value (
    series_id     INT REFERENCES core_energy.fact_series_meta,
    obs_date      DATE NOT NULL,
    value         NUMERIC,
    loaded_at_ts  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (series_id, obs_date)
) PARTITION BY RANGE (obs_date);

-- Partitions from 1980 to 2040 in 5-year chunks
CREATE TABLE IF NOT EXISTS core_energy.fact_series_value_1980_1985
  PARTITION OF core_energy.fact_series_value
  FOR VALUES FROM ('1980-01-01') TO ('1985-01-01');

CREATE TABLE IF NOT EXISTS core_energy.fact_series_value_1985_1990
  PARTITION OF core_energy.fact_series_value
  FOR VALUES FROM ('1985-01-01') TO ('1990-01-01');

CREATE TABLE IF NOT EXISTS core_energy.fact_series_value_1990_1995
  PARTITION OF core_energy.fact_series_value
  FOR VALUES FROM ('1990-01-01') TO ('1995-01-01');

CREATE TABLE IF NOT EXISTS core_energy.fact_series_value_1995_2000
  PARTITION OF core_energy.fact_series_value
  FOR VALUES FROM ('1995-01-01') TO ('2000-01-01');

CREATE TABLE IF NOT EXISTS core_energy.fact_series_value_2000_2005
  PARTITION OF core_energy.fact_series_value
  FOR VALUES FROM ('2000-01-01') TO ('2005-01-01');

CREATE TABLE IF NOT EXISTS core_energy.fact_series_value_2005_2010
  PARTITION OF core_energy.fact_series_value
  FOR VALUES FROM ('2005-01-01') TO ('2010-01-01');

CREATE TABLE IF NOT EXISTS core_energy.fact_series_value_2010_2015
  PARTITION OF core_energy.fact_series_value
  FOR VALUES FROM ('2010-01-01') TO ('2015-01-01');

CREATE TABLE IF NOT EXISTS core_energy.fact_series_value_2015_2020
  PARTITION OF core_energy.fact_series_value
  FOR VALUES FROM ('2015-01-01') TO ('2020-01-01');

CREATE TABLE IF NOT EXISTS core_energy.fact_series_value_2020_2025
  PARTITION OF core_energy.fact_series_value
  FOR VALUES FROM ('2020-01-01') TO ('2025-01-01');

CREATE TABLE IF NOT EXISTS core_energy.fact_series_value_2025_2030
  PARTITION OF core_energy.fact_series_value
  FOR VALUES FROM ('2025-01-01') TO ('2030-01-01');

CREATE TABLE IF NOT EXISTS core_energy.fact_series_value_2030_2035
  PARTITION OF core_energy.fact_series_value
  FOR VALUES FROM ('2030-01-01') TO ('2035-01-01');

CREATE TABLE IF NOT EXISTS core_energy.fact_series_value_2035_2040
  PARTITION OF core_energy.fact_series_value
  FOR VALUES FROM ('2035-01-01') TO ('2040-01-01');
