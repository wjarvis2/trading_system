-- crude balance driver staging & forecast objects
CREATE SCHEMA IF NOT EXISTS model;

-- 1. Driver staging
CREATE TABLE IF NOT EXISTS model.crude_balance_driver (
    obs_date     DATE    NOT NULL,
    series_code  TEXT    NOT NULL,
    value        NUMERIC NOT NULL,
    PRIMARY KEY (obs_date, series_code)
);

-- 2. Forecast output
CREATE TABLE IF NOT EXISTS model.crude_balance_fcast (
    obs_date               DATE    PRIMARY KEY,
    ending_stocks_forecast NUMERIC NOT NULL,
    created_at_ts          TIMESTAMPTZ DEFAULT now()
);

-- 3. Materialised view: realised vs. forecast
CREATE MATERIALIZED VIEW IF NOT EXISTS model.v_crude_balance_full AS
SELECT
    s.date                           AS obs_date,
    s.ending_total_crude_stocks      AS actual_ending_stocks,
    f.ending_stocks_forecast,
    (s.ending_total_crude_stocks - f.ending_stocks_forecast) AS residual
FROM   core_energy.v_series_wide      s
LEFT   JOIN model.crude_balance_fcast f
       ON f.obs_date = s.date;

CREATE UNIQUE INDEX IF NOT EXISTS pk_v_crude_balance_full
    ON model.v_crude_balance_full (obs_date);

