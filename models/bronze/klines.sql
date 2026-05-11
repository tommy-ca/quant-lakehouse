MODEL (
  name bronze.klines,
  kind VIEW,
  owner tommyk,
);

-- Exposes dlt-ingested bronze klines as a SQLMesh model.
-- dlt writes to the 'bronze' schema in catalog.duckdb; this model
-- makes it available to downstream silver models.
SELECT
  open_time,
  CAST(open AS DOUBLE) AS open,
  CAST(high AS DOUBLE) AS high,
  CAST(low AS DOUBLE) AS low,
  CAST(close AS DOUBLE) AS close,
  CAST(volume AS DOUBLE) AS volume,
  CAST(quote_volume AS DOUBLE) AS quote_volume,
  CAST(count AS BIGINT) AS trade_count,
  CAST(taker_buy_volume AS DOUBLE) AS taker_buy_volume,
  CAST(taker_buy_quote_volume AS DOUBLE) AS taker_buy_quote_volume,
  symbol,
  interval
FROM bronze.klines
WHERE open_time IS NOT NULL
