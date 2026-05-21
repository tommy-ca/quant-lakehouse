MODEL (
  name silver.funding_rate,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column ts_event
  ),
  owner tommyk,
  audits (
    not_null(ts_event),
    not_null(funding_rate)
  ),
);

-- Silver funding_rate: Bronze → Silver transform (SQLMesh path).
-- Matches schema in docs/silver-layer-spec.md and funding_rate.py.
SELECT
  CAST(funding_time AS BIGINT) * 1000 AS ts_event,
  CAST(EPOCH_ms(CURRENT_TIMESTAMP) * 1000 AS BIGINT) AS ts_recv,
  CAST(funding_rate AS DOUBLE) AS funding_rate,
  CAST(mark_price AS DOUBLE) AS mark_price,
  CAST(funding_time AS BIGINT) * 1000 AS funding_timestamp,
  'dlt_api' AS source,
  'binance-perps-um' AS exchange,
  'um' AS trade_type,
  symbol,
  'fundingRate' AS data_type,
  CAST(EPOCH_ms(CURRENT_TIMESTAMP) * 1000 AS BIGINT) AS ingested_at,
  CAST(CAST(funding_time AS BIGINT) / 86400000 AS DATE) AS ts_date
FROM bronze.funding_rate
WHERE funding_time BETWEEN @start_ds AND @end_ds
