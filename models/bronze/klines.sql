MODEL (
  name bronze.klines,
  kind VIEW,
  owner tommyk,
);

-- Exposes dlt-ingested bronze klines as a SQLMesh model.
-- Reads from the physical DuckDB table created by dlt in the 'bronze' schema.
-- The table name matches the dlt resource name (e.g. BTCUSDT_klines).
-- SQLMesh resolves the physical table because this model has no upstream
-- SQLMesh dependencies — it references the DuckDB schema directly.
-- NOTE: This model assumes dlt loaded into a single table. If using
-- build_binance_source with multiple symbols, the table is named per symbol
-- and this VIEW must be adjusted accordingly.
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
FROM bronze.BTCUSDT_klines
WHERE open_time IS NOT NULL
