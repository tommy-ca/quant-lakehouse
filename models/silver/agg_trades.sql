MODEL (
  name silver.agg_trades,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column ts_event
  ),
  owner tommyk,
  audits (
    not_null(ts_event),
    not_null(price),
    assert_positive(size)
  ),
);

-- Silver agg_trades: Bronze → Silver transform (SQLMesh path).
-- Matches schema in docs/silver-layer-spec.md and agg_trades.py.
SELECT
  CAST(transact_time AS BIGINT) * 1000 AS ts_event,
  CAST(EPOCH_ms(CURRENT_TIMESTAMP) * 1000 AS BIGINT) AS ts_recv,
  CAST(price AS DOUBLE) AS price,
  CAST(quantity AS DOUBLE) AS size,
  CASE WHEN is_buyer_maker = FALSE THEN 'buy' ELSE 'sell' END AS side,
  CAST(agg_trade_id AS BIGINT) AS trade_id,
  CASE WHEN is_buyer_maker = TRUE THEN 1 ELSE 0 END AS is_buyer_maker,
  CAST(agg_trade_id AS BIGINT) AS agg_trade_id,
  CAST(first_trade_id AS BIGINT) AS first_trade_id,
  CAST(last_trade_id AS BIGINT) AS last_trade_id,
  'agg' AS rtype,
  'dlt_api' AS source,
  'binance-spot' AS exchange,
  'spot' AS trade_type,
  symbol,
  'aggTrades' AS data_type,
  CAST(EPOCH_ms(CURRENT_TIMESTAMP) * 1000 AS BIGINT) AS ingested_at,
  CAST(CAST(transact_time AS BIGINT) / 86400000 AS DATE) AS ts_date
FROM bronze.agg_trades
WHERE transact_time BETWEEN @start_ds AND @end_ds
