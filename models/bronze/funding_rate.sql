MODEL (
  name bronze.funding_rate,
  kind VIEW,
  owner tommyk,
);

-- Exposes dlt-ingested bronze fundingRate as a SQLMesh model.
SELECT
  symbol,
  CAST(funding_time AS BIGINT) AS funding_time,
  CAST(funding_rate AS DOUBLE) AS funding_rate,
  CAST(mark_price AS DOUBLE) AS mark_price
FROM bronze.funding_rate
WHERE funding_time IS NOT NULL
