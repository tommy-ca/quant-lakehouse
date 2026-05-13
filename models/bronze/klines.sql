MODEL (
  name bronze.klines,
  kind VIEW,
  owner tommyk,
);

-- Exposes dlt-ingested bronze klines as a SQLMesh model.
-- Reads from the unified DuckDB table created by dlt in the 'bronze' schema.
-- All symbols write to a single bronze.klines table via build_binance_source()
-- which applies .apply_hints(table_name="klines") to each resource.
SELECT
  CAST(open_time AS BIGINT) AS open_time,
  CAST(open AS DOUBLE) AS open,
  CAST(high AS DOUBLE) AS high,
  CAST(low AS DOUBLE) AS low,
  CAST(close AS DOUBLE) AS close,
  CAST(volume AS DOUBLE) AS volume,
  CAST(close_time AS BIGINT) AS close_time,
  CAST(quote_volume AS DOUBLE) AS quote_volume,
  CAST(count AS BIGINT) AS trade_count,
  CAST(taker_buy_volume AS DOUBLE) AS taker_buy_volume,
  CAST(taker_buy_quote_volume AS DOUBLE) AS taker_buy_quote_volume,
  symbol,
  interval
FROM bronze.klines
WHERE open_time IS NOT NULL
