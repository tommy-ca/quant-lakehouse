MODEL (
  name bronze.agg_trades,
  kind VIEW,
  owner tommyk,
);

-- Exposes dlt-ingested bronze aggTrades as a SQLMesh model.
SELECT
  CAST(agg_trade_id AS BIGINT) AS agg_trade_id,
  CAST(price AS DOUBLE) AS price,
  CAST(quantity AS DOUBLE) AS quantity,
  CAST(transact_time AS BIGINT) AS transact_time,
  CAST(is_buyer_maker AS BOOLEAN) AS is_buyer_maker,
  CAST(first_trade_id AS BIGINT) AS first_trade_id,
  CAST(last_trade_id AS BIGINT) AS last_trade_id,
  symbol
FROM bronze.agg_trades
WHERE agg_trade_id IS NOT NULL
