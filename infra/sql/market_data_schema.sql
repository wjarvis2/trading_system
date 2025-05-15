-- Enable Timescale (once per database)
CREATE EXTENSION IF NOT EXISTS timescaledb;

------------------------------------------------------------------
-- 0. Namespace for all market-data objects
------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS market_data;
SET search_path = market_data, public;

------------------------------------------------------------------
-- 1. Dimension tables
------------------------------------------------------------------
CREATE TABLE contract (
    contract_id  BIGSERIAL PRIMARY KEY,
    root         TEXT    NOT NULL,
    exchange     TEXT    NOT NULL,
    expiry       DATE    NOT NULL,
    contract_code TEXT   NOT NULL,
    multiplier   NUMERIC NOT NULL,
    tick_size    NUMERIC NOT NULL,
    currency     TEXT    NOT NULL,
    UNIQUE (contract_code)
);

CREATE TABLE option_contract (
    option_id    BIGSERIAL PRIMARY KEY,
    future_id    BIGINT  NOT NULL REFERENCES contract(contract_id),
    strike       NUMERIC NOT NULL,
    right        CHAR(1) NOT NULL CHECK (right IN ('C','P')),
    expiry       DATE    NOT NULL,
    local_code   TEXT    NOT NULL,
    UNIQUE (future_id, strike, right, expiry)
);

------------------------------------------------------------------
-- 2. Tick hypertables
------------------------------------------------------------------
CREATE TABLE trade_tick (
    ts          TIMESTAMPTZ NOT NULL,
    contract_id BIGINT      NOT NULL REFERENCES contract(contract_id),
    price       NUMERIC     NOT NULL,
    size        NUMERIC     NOT NULL,
    PRIMARY KEY (ts, contract_id)
);
SELECT create_hypertable('trade_tick','ts',
        chunk_time_interval => INTERVAL '1 day',
        if_not_exists => TRUE);

CREATE TABLE quote_tick (
    ts          TIMESTAMPTZ NOT NULL,
    contract_id BIGINT      NOT NULL REFERENCES contract(contract_id),
    bid         NUMERIC,
    ask         NUMERIC,
    bid_size    NUMERIC,
    ask_size    NUMERIC,
    PRIMARY KEY (ts, contract_id)
);
SELECT create_hypertable('quote_tick','ts',
        chunk_time_interval => INTERVAL '1 day',
        if_not_exists => TRUE);

------------------------------------------------------------------
-- 3. Minute bars (aggregated)
------------------------------------------------------------------
CREATE TABLE bar_1m (
    bucket       TIMESTAMPTZ NOT NULL,
    contract_id  BIGINT      NOT NULL REFERENCES contract(contract_id),
    open         NUMERIC     NOT NULL,
    high         NUMERIC     NOT NULL,
    low          NUMERIC     NOT NULL,
    close        NUMERIC     NOT NULL,
    volume       NUMERIC,
    PRIMARY KEY (bucket, contract_id)
);
SELECT create_hypertable('bar_1m','bucket',
        chunk_time_interval => INTERVAL '7 days',
        if_not_exists => TRUE);

------------------------------------------------------------------
-- 4. Options tick table (greeks + IV)
------------------------------------------------------------------
CREATE TABLE option_quote_tick (
    ts            TIMESTAMPTZ NOT NULL,
    option_id     BIGINT      NOT NULL REFERENCES option_contract(option_id),
    bid           NUMERIC,
    ask           NUMERIC,
    bid_size      NUMERIC,
    ask_size      NUMERIC,
    implied_vol   NUMERIC,
    delta         NUMERIC,
    gamma         NUMERIC,
    theta         NUMERIC,
    vega          NUMERIC,
    rho           NUMERIC,
    volume        NUMERIC,
    open_interest NUMERIC,
    underlying_px NUMERIC,
    PRIMARY KEY (ts, option_id)
);
SELECT create_hypertable('option_quote_tick','ts',
        chunk_time_interval => INTERVAL '1 day',
        if_not_exists => TRUE);

------------------------------------------------------------------
-- 5. Compression policies  (optional but recommended)
------------------------------------------------------------------
ALTER TABLE trade_tick
  SET (timescaledb.compress,
       timescaledb.compress_segmentby = 'contract_id',
       timescaledb.compress_orderby   = 'ts DESC');
SELECT add_compression_policy('trade_tick', INTERVAL '14 days');

ALTER TABLE quote_tick
  SET (timescaledb.compress,
       timescaledb.compress_segmentby = 'contract_id',
       timescaledb.compress_orderby   = 'ts DESC');
SELECT add_compression_policy('quote_tick', INTERVAL '14 days');

ALTER TABLE option_quote_tick
  SET (timescaledb.compress,
       timescaledb.compress_segmentby = 'option_id',
       timescaledb.compress_orderby   = 'ts DESC');
SELECT add_compression_policy('option_quote_tick', INTERVAL '14 days');
