---
date: 2026-05-11
topic: bronze-silver-gold
status: active
---

# Bronze → Silver → Gold Architecture: Implementation Plan

## Layer Definitions

| Layer | Name | Content | Transform | Schema Contract |
|-------|------|---------|-----------|----------------|
| **Bronze** | `bronze.*` | Upstream mirror — raw VARCHAR columns, no type casting, no field drops | `write_disposition="merge"`, `schema_contract="evolve"` | Accept anything from upstream |
| **Silver** | `silver.*` | Production-ready typed data — float/int columns, Pandera validated, DBN/tardis naming conventions | `schema_contract="freeze"` | Strict — reject bad data |
| **Gold** | `gold.*` | Feature engineering, analytics views, ML datasets (future) | Per-application | Per-application |

## Bronze Layer Design

Three sub-layers by source:

| Source | Storage | Format | dlt Write Disposition | Schema Contract | Primary Key |
|--------|---------|--------|----------------------|----------------|-------------|
| **Archive** | Filesystem (`{archive_home}/data/...`) | Zipped CSV files (same as upstream S3) | `replace` for file index table | `evolve` | `(file_url)` |
| **REST API** | DuckDB `bronze.rest_*` | All VARCHAR columns (no type casting) | `merge` | `evolve` | `(symbol, open_time/etc)` |
| **WS** | DuckDB `bronze.ws_*` | All VARCHAR columns (raw message text) | `append` | `evolve` | None |

## Implementation Phases

### Phase 11: Bronze Raw VARCHAR + Silver Promotion (current)

**Goal**: REST/WS dlt sources write raw VARCHAR data to bronze; Silver transforms parse VARCHAR→typed; no lost fields.

**Tasks**:
| ID | Task | Est. |
|----|------|------|
| 11.1 | Create raw VARCHAR Pydantic models (`RawKlineModel`, `RawAggTradeModel`, `RawFundingRateModel`) | 0.5 session |
| 11.2 | Update REST/WS dlt sources to use raw models + `schema_contract="evolve"` | 0.5 session |
| 11.3 | Update Silver transforms to parse VARCHAR→typed as first step | 1 session |
| 11.4 | Restore `first_trade_id`, `last_trade_id` in agg_trades silver; restore `mark_price` in funding_rate silver | 0.5 session |
| 11.5 | Tests + lint + commit | 0.5 session |

### Phase 12: Archive Bronze Index (next)

**Goal**: dlt filesystem source indexes the local archive mirror; bronze.archive_files metadata table tracks available data.

**Tasks**:
| ID | Task | Est. |
|----|------|------|
| 12.1 | Create `bronze_archive_index` dlt source using `dlt.filesystem` | 1 session |
| 12.2 | Create `bronze.archive_files` metadata table with symbol/data_type/interval/date tracking | 0.5 session |
| 12.3 | Create Prefect `refresh_archive_index` flow (daily cron) | 0.5 session |

### Phase 13: Gold Layer (future)

**Goal**: Feature engineering views, analytics datasets, ML-ready data products.

**Tasks**:
| ID | Task | Est. |
|----|------|------|
| 13.1 | Design gold views per analytics requirements | 1 session |
| 13.2 | Implement gold views as SQLMesh models or Polars transforms | 2 sessions |

## Table Schemas

### Bronze Tables (all VARCHAR)

**`bronze.rest_klines`** / **`bronze.ws_klines`**:
```sql
open_time TEXT, open TEXT, high TEXT, low TEXT, close TEXT, volume TEXT,
close_time TEXT, quote_volume TEXT, count TEXT,
taker_buy_volume TEXT, taker_buy_quote_volume TEXT,
symbol TEXT, interval TEXT
```

**`bronze.rest_agg_trades`**:
```sql
agg_trade_id TEXT, price TEXT, quantity TEXT,
first_trade_id TEXT, last_trade_id TEXT, transact_time TEXT,
is_buyer_maker TEXT, symbol TEXT
```

**`bronze.rest_funding_rate`**:
```sql
symbol TEXT, funding_time TEXT, funding_rate TEXT, mark_price TEXT
```

### Silver Tables (typed, current promoted)

**`silver.klines`** — 19 columns (current, unchanged):
```sql
ts_event BIGINT, ts_recv BIGINT, open DOUBLE, high DOUBLE, low DOUBLE,
close DOUBLE, volume DOUBLE, quote_volume DOUBLE, trade_count BIGINT,
taker_buy_volume DOUBLE, taker_buy_quote_volume DOUBLE,
source TEXT, exchange TEXT, trade_type TEXT, symbol TEXT,
interval TEXT, data_type TEXT, ingested_at BIGINT, ts_date DATE
```

**`silver.agg_trades`** — 18 columns (adds `first_trade_id`, `last_trade_id`):
```sql
(prev columns) + first_trade_id BIGINT, last_trade_id BIGINT
```

**`silver.funding_rate`** — 13 columns (adds `mark_price` from API):
```sql
(prev columns) + mark_price DOUBLE  -- from API, not placeholder 0.0
```

## Design Decisions

1. **REST bronze uses VARCHAR models, not typed models** — RawKlineModel is all TEXT fields, no validators. Schema enforcement happens at the Silver boundary, not the Bronze boundary.

2. **Archive index is separate from archive data** — The filesystem index (`bronze.archive_files`) tracks file metadata. The actual data flows ZIP→Polars→Silver directly (no intermediate VARCHAR DuckDB table for archive data).

3. **Current bronze tables stay in place** — Old typed data in `bronze.klines` coexists with new VARCHAR data. Silver transforms handle both input formats (the VARCHAR→Float64 cast handles numeric strings and numeric types transparently).

4. **Gold layer deferred** — No gold work until bronze+silver are stable and producing data.

## Migration Path

| Step | Action | Data Safety |
|------|--------|-------------|
| 1 | Add raw VARCHAR models | New models alongside existing — no changes to existing code |
| 2 | Update dlt REST sources to use raw models | dlt creates new tables in bronze schema with VARCHAR columns; old typed tables remain |
| 3 | Update Silver transforms | VARCHAR→typed parse added as first transform step; existing typed data passes through unchanged |
| 4 | Restore lost fields | `first_trade_id`, `last_trade_id`, `mark_price` added to Pydantic models and silver schemas |
| 5 | Tests | New tests for raw→silver transform path; existing tests for typed→silver path |
