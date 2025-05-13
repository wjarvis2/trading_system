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

-- 3. Materialised view: realised vs forecast (needs v_model_wide)
CREATE MATERIALIZED VIEW IF NOT EXISTS model.v_crude_balance_full AS
SELECT
    w.obs_date,
    w.ending_total_crude_stocks                         AS actual_ending_stocks,
    f.ending_stocks_forecast,
    (w.ending_total_crude_stocks - f.ending_stocks_forecast) AS residual
FROM   core_energy.v_model_wide          w
LEFT   JOIN model.crude_balance_fcast    f  USING (obs_date);

CREATE UNIQUE INDEX IF NOT EXISTS pk_v_crude_balance_full
    ON model.v_crude_balance_full(obs_date);
