# Metadata Lake Reference

This document describes the high-fidelity instrument and venue metadata stored in the Lakehouse (`registry` schema). This metadata is designed to eliminate survivorship bias in backtesting and provide a definitive temporal map of asset availability.

## 1. Registry Tables

### `registry.venues`
Catalog of trading environments.

| Field | Type | Description |
| :--- | :--- | :--- |
| `venue_id` | `VARCHAR` | Primary Key (e.g., `binance_spot`). |
| `name` | `VARCHAR` | Display name. |
| `market_type` | `VARCHAR` | `spot`, `um`, `cm`. |
| `base_url` | `VARCHAR` | API endpoint. |
| `status` | `VARCHAR` | Current operational status. |
| `fetched_at` | `BIGINT` | Unix timestamp (ms). |

### `registry.instruments`
Unified instrument specifications for all market segments.

| Field | Type | Description | Fidelity |
| :--- | :--- | :--- | :--- |
| `symbol` | `VARCHAR` | Native symbol identifier. | High |
| `venue_id` | `VARCHAR` | FK to `venues`. | High |
| `base_asset` | `VARCHAR` | Base currency. | High |
| `quote_asset` | `VARCHAR` | Quote currency. | High |
| `status` | `VARCHAR` | `trading`, `delisted`, `settling`. | High |
| `tick_size` | `DOUBLE` | Minimum price step. | High |
| `lot_size` | `DOUBLE` | Minimum quantity step. | High |
| `min_notional` | `DOUBLE` | Minimum order value in quote asset. | Medium |
| `price_precision`| `INTEGER` | Decimal places for prices. | High |
| `qty_precision` | `INTEGER` | Decimal places for quantities. | High |
| `contract_type` | `VARCHAR` | `spot`, `perpetual`, `delivery`.| High |
| `onboard_date` | `BIGINT` | Contract listing/onboard time (ms). | High (Futures) |
| `delivery_date`| `BIGINT` | Contract expiry/delivery time (ms). | High (Futures) |
| `first_data_at`| `BIGINT` | First available data date in S3 (ms). | Inferred |
| `last_data_at` | `BIGINT` | Last available data date in S3 (ms). | Inferred |

## 2. Temporal Metadata & Backtesting
To avoid survivorship bias, backtests should query `registry.instruments` to determine which assets were tradable at any given `T`.

### Example: Unbiased Universe Selection
```sql
SELECT symbol
FROM registry.instruments
WHERE venue_id = 'binance_spot'
  AND (onboard_date IS NULL OR onboard_date <= :target_time)
  AND (delivery_date IS NULL OR delivery_date >= :target_time)
  AND (status = 'trading' OR last_data_at >= :target_time)
```

## 3. Interoperability Views
Mapped for external ecosystem compatibility.

- `registry.v_databento`: Fields mapped to DBN conventions.
- `registry.v_tardis`: Fields mapped to Tardis.dev conventions.
