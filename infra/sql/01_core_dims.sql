CREATE SCHEMA IF NOT EXISTS core_energy;

CREATE TABLE core_energy.dim_source (
    source_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    url TEXT,
    default_freq TEXT
);

CREATE TABLE core_energy.fact_series_meta (
    series_id  SERIAL PRIMARY KEY,
    series_code TEXT UNIQUE,
    source_id   INT REFERENCES core_energy.dim_source,
    description TEXT
);

CREATE TABLE core_energy.fact_series_value (
    series_id INT REFERENCES core_energy.fact_series_meta,
    obs_date  DATE NOT NULL,
    value     NUMERIC,
    loaded_at_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (series_id, obs_date, loaded_at_ts)
) PARTITION BY RANGE (obs_date);

-- first  partition (2020‑2026); add more later
CREATE TABLE core_energy.fact_series_value_2020_2026
  PARTITION OF core_energy.fact_series_value
  FOR VALUES FROM ('2020-01-01') TO ('2026-01-01');

