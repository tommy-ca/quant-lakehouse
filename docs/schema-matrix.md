# Data Schema Matrix: Cross-Reference of Market Data Formats

## Overview

This document maps field names and conventions across four data sources:
- **Binance Archive** (data.binance.vision CSV)
- **Binance REST/WS API** (official SDK)
- **tardis.dev** (normalized CSV)
- **Databento DBN** (binary encoding)

Our Silver layer schema is shown with its source convention for each field.

---

## 1. OHLCV / Klines

| binance-datatool | Binance Archive CSV | Binance REST API | tardis.dev | DBN (OhlcvMsg) |
|---|---|---|---|---|
| `ts_event` INT64 μs | `open_time` | `[0]` | `timestamp` (μs) | `ts_event` (ns) |
| `ts_recv` INT64 μs | — | — | `local_timestamp` (μs) | `ts_recv` (ns) |
| `open` FLOAT64 | `open` | `[1]` | — | `open` i64 1e-9 |
| `high` FLOAT64 | `high` | `[2]` | — | `high` i64 1e-9 |
| `low` FLOAT64 | `low` | `[3]` | — | `low` i64 1e-9 |
| `close` FLOAT64 | `close` | `[4]` | — | `close` i64 1e-9 |
| `volume` FLOAT64 | `volume` | `[5]` | — | `volume` u64 |
| `quote_volume` FLOAT64 | `quote_volume` | `[7]` | — | — |
| `trade_count` INT64 | `count` | `[8]` | — | — |
| `taker_buy_volume` FLOAT64 | `taker_buy_volume` | `[9]` | — | — |
| `taker_buy_quote_volume` FLOAT64 | `taker_buy_quote_volume` | `[10]` | — | — |
| `exchange` VARCHAR | — | — | `exchange` | `publisher_id` u16 |
| `trade_type` VARCHAR | file path | — | — | `publisher_id` (encoded) |
| `symbol` VARCHAR | file name | `symbol` | `symbol` | `instrument_id` u32 |
| `interval` VARCHAR | file name | `interval` | — | `rtype` (OHLCV_1H etc.) |
| `data_type` VARCHAR | file path | — | — | `schema` |
| `ingested_at` INT64 μs | — | — | — | — |
| `source` VARCHAR | — | — | — | `schema` |
| `ts_date` DATE | derived | derived | — | `ts_event` date part |


**Timestamp comparison**: `1778256000000` (ms) = `1778256000000000` (μs tardis.dev) = `1778256000000000000` (ns DBN)

**Key difference**: DBN uses i64 fixed-point prices (1 unit = 1e-9), Binance uses decimal strings. Our choice: FLOAT64.

---

## 2. Trades / AggTrades

| binance-datatool | Binance Archive CSV | Binance REST API | tardis.dev | DBN (TradeMsg) |
|---|---|---|---|---|
| `ts_event` INT64 μs | `transact_time` | `T` | `timestamp` (μs) | `ts_event` (ns) |
| `ts_recv` INT64 μs | — | — | `local_timestamp` (μs) | `ts_recv` (ns) |
| `price` FLOAT64 | `price` | `p` | `price` | `price` i64 1e-9 |
| `size` FLOAT64 | `quantity` | `q` | `amount` | `size` u32 |
| `side` VARCHAR | `is_buyer_maker` | `m` | `side` | `action` + `side` |
| `trade_id` INT64 | `agg_trade_id` | `a` / `A` | `id` | `sequence` u32 |
| `is_buyer_maker` INT64 | `is_buyer_maker` | `m` | — | `side` (encoded) |
| `agg_trade_id` INT64 | `agg_trade_id` | `a` | — | — |
| `first_trade_id` INT64 | `first_trade_id` | `f` | — | — |
| `last_trade_id` INT64 | `last_trade_id` | `l` | — | — |
| `rtype` VARCHAR | — | — | — | `rtype` u8 (MBP_0) |
| `exchange` VARCHAR | — | — | `exchange` | `publisher_id` u16 |
| `trade_type` VARCHAR | file path | — | — | `publisher_id` (encoded) |
| `symbol` VARCHAR | file name | `s` | `symbol` | `instrument_id` u32 |
| `data_type` VARCHAR | file path | — | — | `schema` (Trades) |
| `ingested_at` INT64 μs | — | — | — | — |
| `source` VARCHAR | — | — | — | `schema` |
| `ts_date` DATE | derived | derived | — | `ts_event` date part |

**tardis.dev `amount`**: Our `size` maps to tardis.dev `amount`. DBN also uses `size`. Binance uses `quantity`.

**Side semantics**:
- tardis.dev: `side` = liquidity taker side (`buy`/`sell`)
- DBN: `action` (A/C/M/R/T/F) + `side` (A/B/N for ask/bid/none)
- Binance: `is_buyer_maker` (bool: was buyer the maker?)

---

## 3. Funding Rate (Derivatives)

| binance-datatool | Binance Archive CSV | Binance REST API | tardis.dev | DBN |
|---|---|---|---|---|
| `ts_event` INT64 μs | `funding_time` | `fundingTime` | `timestamp` (μs) | — |
| `ts_recv` INT64 μs | — | — | `local_timestamp` (μs) | — |
| `funding_rate` FLOAT64 | `funding_rate` | `fundingRate` | `funding_rate` | — |
| `mark_price` FLOAT64 | `mark_price` | `markPrice` | `mark_price` | — |
| `funding_timestamp` INT64 μs | `funding_time` | `fundingTime` | `funding_timestamp` (μs) | — |
| `exchange` VARCHAR | — | — | `exchange` | — |
| `symbol` VARCHAR | `symbol` | `symbol` | `symbol` | — |
| `trade_type` VARCHAR | file path | — | — | — |
| `data_type` VARCHAR | file path | — | — | — |
| `ingested_at` INT64 μs | — | — | — | — |
| `source` VARCHAR | — | — | — | — |
| `ts_date` DATE | derived | derived | — | — |

**tardis.dev has additional fields** we could add:
- `predicted_funding_rate`: next-next funding rate estimate
- `open_interest`: current open interest
- `index_price`: underlying index price
- `last_price`: last trade price

**DBN**: No native funding rate schema. Funding data may be in `StatMsg` with `StatType::OpenInterest` etc.

---

## 4. Exchange / Symbol References

| Concept | binance-datatool | tardis.dev | DBN |
|---|---|---|---|
| Exchange ID | `exchange` VARCHAR: `"binance-spot"`, `"binance-perps-um"`, `"binance-perps-cm"` | `exchange` VARCHAR: same values | `publisher_id` u16: numeric IDs |
| Market type | `trade_type` VARCHAR: `"spot"`, `"um"`, `"cm"` | Encoded in exchange ID | Encoded in `publisher_id` |
| Trading pair | `symbol` VARCHAR: `"BTCUSDT"` | `symbol` VARCHAR: same | `instrument_id` u32 + `raw_symbol` |

**tardis.dev exchange mapping**:
| tardis.dev ID | binance-datatool `exchange` | binance-datatool `trade_type` |
|---|---|---|
| `binance` | `binance-spot` | `spot` |
| `binance-perps-um` | `binance-perps-um` | `um` |
| `binance-perps-cm` | `binance-perps-cm` | `cm` |

---

## 5. Timestamp Units

| Source | Unit | Example (2026-05-08 00:00:00 UTC) |
|---|---|---|
| binance-datatool | **μs** | `1778256000000000` |
| Binance archive | **ms** | `1778256000000` |
| Binance REST API | **ms** | `1778256000000` |
| tardis.dev | **μs** | `1778256000000000` |
| DBN | **ns** | `1778256000000000000` |

**Conversion**: multiply/divide by 1000 between adjacent units. binance-datatool stores timestamps as `ts_event`/`ts_recv` in epoch μs (INT64).

---

## 6. Type Representation

| Concept | binance-datatool | Binance Archive | Binance REST | tardis.dev | DBN |
|---|---|---|---|---|---|
| Timestamps | `INT64` μs | `INT64` ms | `INT64` ms | `INT64` μs | `u64` ns |
| Prices | `FLOAT64` | `str` (decimal) | `str` (decimal) | `FLOAT64` | `i64` 1e-9 |
| Volumes | `FLOAT64` | `str` (decimal) | `str` (decimal) | `INT64` | `u32` |
| Counts | `INT64` | `INT64` | `INT64` | `INT64` | `u32` |
| Strings | `VARCHAR` | `str` | `str` | `str` | `[c_char; N]` |

**Our choice**: FLOAT64 for prices/volumes (same as tardis.dev), INT64 for counts/timestamps.
DBN uses fixed-point i64 for prices (1e-9 precision) — more precise but less intuitive.

---

## 7. Schema Coverage by Data Type

| Data Type | Binance Archive | Binance REST/WS | tardis.dev | DBN |
|---|---|---|---|---|
| klines (OHLCV) | ✅ daily CSV | ✅ `klines()` | ❌ (tick-level only) | ✅ `OhlcvMsg` |
| aggTrades | ✅ daily CSV | ✅ `agg_trades()` | ✅ `trades` CSV | ✅ `TradeMsg` (MBP_0) |
| trades | ✅ daily CSV | ✅ `historical_trades()` | ✅ `trades` CSV | ✅ `TradeMsg` |
| fundingRate | ✅ monthly CSV | ✅ `get_funding_rate_history()` | ✅ `derivative_ticker` CSV | ❌ (via `StatMsg`) |
| bookDepth | ✅ daily CSV | ❌ (WS only) | ✅ `incremental_book_L2` CSV | ✅ `MbpMsg` |
| bookTicker | ✅ daily CSV | ❌ (WS only) | ✅ `book_ticker` CSV | ✅ `BboMsg` |
| indexPriceKlines | ✅ daily CSV | ✅ `index_price_klines()` | ❌ | ❌ |
| markPriceKlines | ✅ daily CSV | ✅ `mark_price_klines()` | ❌ | ❌ |

---

## 8. Our Convention Priority

```
1. tardis.dev    → exchange naming, price/size/side, timestamp epoch
2. DBN           → ts_event/ts_recv, rtype, size (trade amount)
3. Binance       → klines-specific fields (quote_volume, trade_count)
```

See `docs/silver-layer-spec.md` -> Design Principles for the full rationale.
