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

-- Silver klines: Bronze → Silver transform.
-- Renames columns to DBN conventions, adds metadata, casts types.
-- Incremental by time range for efficient backfill.
SELECT
  CAST(open_time AS BIGINT) AS ts_event,
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
  CASE
    WHEN REGEXP_MATCHES(symbol, '[A-Z]+(USDT|BUSD|USDC)$') THEN 'binance-spot'
    ELSE 'binance-spot'
  END AS exchange,
  'spot' AS trade_type,
  symbol,
  interval,
  'klines' AS data_type,
  CAST(EPOCH_ms(CURRENT_TIMESTAMP) * 1000 AS BIGINT) AS ingested_at,
  CAST(CAST(CAST(open_time / 1000000 AS BIGINT) AS DATE) AS DATE) AS ts_date
FROM bronze.klines
WHERE CAST(open_time AS BIGINT) BETWEEN @start_ds AND @end_ds
