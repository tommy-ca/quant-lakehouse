MODEL (
  name silver.klines,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column ts_event
  ),
  owner tommyk,
  audits (
    not_null(ts_event),
    not_null(open),
    assert_positive(volume)
  ),
);

-- Silver klines: Bronze → Silver transform (SQLMesh path).
--
-- NOTE: This is a MINIMAL demonstration model. The primary pipeline uses Polars
-- transforms (bronze_klines_to_silver in transforms/klines.py). This SQLMesh
-- model has several limitations compared to the Polars path:
--
--   1. Hardcoded metadata: exchange='binance-spot', trade_type='spot',
--      source='dlt_api' — does not handle um/cm trade types or archive source.
--   2. No μs auto-detection: assumes open_time is in milliseconds (ms).
--      Archive data with μs timestamps (16-digit) produces incorrect ts_date.
--   3. ts_recv/ingested_at: computed at query time, not at ingestion time.
--   4. Only spot klines: no aggTrades or fundingRate models exist.
--
-- For production use, prefer the Polars transform pipeline which handles all
-- trade types, source types, and μs auto-detection.
SELECT
  CAST(CAST(open_time AS BIGINT) AS BIGINT) * 1000 AS ts_event,
  CAST(EPOCH_ms(CURRENT_TIMESTAMP) * 1000 AS BIGINT) AS ts_recv,
  CAST(open AS DOUBLE) AS open,
  CAST(high AS DOUBLE) AS high,
  CAST(low AS DOUBLE) AS low,
  CAST(close AS DOUBLE) AS close,
  CAST(volume AS DOUBLE) AS volume,
  CAST(quote_volume AS DOUBLE) AS quote_volume,
  CAST(trade_count AS BIGINT) AS trade_count,
  CAST(taker_buy_volume AS DOUBLE) AS taker_buy_volume,
  CAST(taker_buy_quote_volume AS DOUBLE) AS taker_buy_quote_volume,
  'dlt_api' AS source,
  'binance-spot' AS exchange,
  'spot' AS trade_type,
  symbol,
  interval,
  'klines' AS data_type,
  CAST(EPOCH_ms(CURRENT_TIMESTAMP) * 1000 AS BIGINT) AS ingested_at,
  CAST(CAST(CAST(open_time AS BIGINT) / 86400000 AS BIGINT) AS DATE) AS ts_date
FROM bronze.klines
WHERE CAST(open_time AS BIGINT) BETWEEN @start_ds AND @end_ds
