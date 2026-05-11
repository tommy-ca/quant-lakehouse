---
date: 2026-05-11
topic: platform-evolution
status: proposal
---

# Platform Evolution: Scalable Crypto Historical Data Engineering Platform

## Problem Frame

The current `binance-datatool` is a focused CLI toolkit for the Binance public archive (S3). It
succeeds at its core mission — listing, downloading, verifying, and gap-filling Binance market
data — but has structural limitations that prevent it from becoming a general-purpose crypto
historical data platform:

1. **Single-source hardcoding**: ArchiveClient, SymbolInfo inference, file path construction, and
   CLI commands all assume Binance S3 structure. Adding a new source (tardis.dev, Databento)
   requires changes across every layer.

2. **Hand-rolled extract logic**: Pagination, retries, rate limiting, schema inference, and
   incremental state tracking are implemented per-endpoint. `dlt` provides all of these as
   declarative configuration.

3. **Transform logic in Python**: Bronze→Silver transforms use ad-hoc Polars DataFrames.
   `SQLMesh` provides SQL-based transformations with automatic incremental tracking, column-level
   lineage, virtual data environments, and built-in data quality audits.

4. **No unified multi-source catalog**: Silver schemas exist but the catalog only ingests Binance
   archive. Multiple sources produce multiple tables with no cross-source views.

5. **No MLOps/feature engineering**: No point-in-time correct datasets, feature store, or
   backtesting-ready data slices for ML workflows.

6. **No real-time streaming sink**: WS clients exist but no stream-to-lake pipeline.

## Vision

A unified, scalable crypto historical data engineering platform that:

- Ingests from **three source tiers**: free archive (Binance S3), professional tick-level
  (tardis.dev, 50+ exchanges), and institutional (Databento, CME/OPRA)
- Uses a **declarative ELT architecture**: `dlt` for Extract/Load, `SQLMesh` for Transform,
  `DuckDB/DuckLake` for Storage, `Prefect` for Orchestration
- Provides **one unified Silver layer**: same schemas regardless of source, with `source` column
  for provenance
- Supports **DataOps**: data contracts, automated quality gates, lineage tracking, freshness SLAs
- Supports **MLOps**: point-in-time correct datasets, feature views, backtesting-ready exports,
  Z-score anomaly detection (already implemented)
- Is **agent-native**: Skills framework for AI-driven discovery, download, transform, and publish
- Follows **SOLID, KISS, DRY, YAGNI** throughout

## Target Architecture

```
                        ┌─────────────────────────────────────────────┐
                        │              Prefect Orchestrator            │
                        │  (schedules, dependencies, retries, DLQ)     │
                        └─────────────────────────────────────────────┘
                                       │
        ┌──────────────────────────────┼──────────────────────────────┐
        │                              │                              │
   ┌────▼─────┐                 ┌──────▼─────┐                 ┌─────▼──────┐
   │  dlt EL   │                 │   dlt EL    │                 │   dlt EL    │
   │ Binance   │                 │  tardis.dev  │                 │  Databento  │
   │ Archive   │                 │  50+ Exchs   │                 │  CME/OPRA   │
   └────┬─────┘                 └──────┬─────┘                 └─────┬──────┘
        │                              │                              │
        └──────────────────────────────┼──────────────────────────────┘
                                       │
                              ┌────────▼────────┐
                              │   Bronze Layer   │
                              │  (dlt normalized)│
                              │  raw per-source  │
                              └────────┬────────┘
                                       │
                              ┌────────▼────────┐
                              │   SQLMesh Transforms │
                              │ Bronze → Silver │
                              │ Audits, Views   │
                              └────────┬────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    │                  │                  │
              ┌─────▼─────┐    ┌──────▼──────┐    ┌──────▼──────┐
              │  Silver    │    │   Feature   │    │   Analytics │
              │  DuckLake  │    │   Store     │    │   Views     │
              │  (ACID)    │    │  (duckdb)   │    │  (dashboards)│
              └───────────┘    └─────────────┘    └─────────────┘
                                       │
                              ┌────────▼────────┐
                              │   Data Ops      │
                              │ Contracts, Lineage│
                              │ Quality Gates   │
                              │ Anomaly Detection│
                              └─────────────────┘
```

## Stack Components

### dlt (Data Load Tool) — Extract & Load

**Role**: Replace hand-rolled pagination, retry, state management, schema inference, and file
formatting for each data source.

**What it provides**:
- Declarative REST API sources (`rest_api_source`) with built-in pagination, auth, rate limiting
- Incremental loading with automatic cursor state persistence
- Schema inference and evolution (4 contract modes: evolve/freeze/discard_row/discard_value)
- Nested JSON normalization (dicts→columns, lists→child tables)
- Thread/process pool parallelism for multi-source extraction
- DuckDB destination with full write dispositions (replace/append/merge)
- Filesystem destination (Parquet/JSONL) with Hive-style partitioning

**Integration with existing code**:
- Wrap `BinanceRestClient.fetch_ohlcv()` as `@dlt.resource` with incremental cursor on `open_time`
- Wrap `tardis.dev` HTTP API as `rest_api_source` with pagination
- Replace `ArchiveDownloadWorkflow` and `GapFillWorkflow` with dlt resources for new sources
- Keep existing workflows as parallel path for backward compatibility

### SQLMesh — Transform

**Role**: Replace ad-hoc Polars Bronze→Silver transforms with SQL-based, incrementally tracked,
column-level lineage transformations.

**What it provides**:
- `INCREMENTAL_BY_TIME_RANGE` — maps directly to time-series market data with interval tracking
- Automatic backfill scope — only compute what changed (surgical, not full-refresh)
- Virtual data environments — zero-copy dev environments that reuse prod data
- Column-level lineage — automatically detect non-breaking vs breaking schema changes
- Built-in audits — data quality checks that run before apply (fail fast)
- Forward-only changes — apply to new data without backfilling terabytes of history
- DuckDB-native — `sqlmesh init duckdb` for instant setup

**Integration with existing code**:
- Current `SinkWorkflow._bronze_*_to_silver()` functions become SQLMesh models
- Existing Silver schema definitions in `sink.py` become SQLMesh model SQL
- Audits replace `check_ducklake_anomalies()` for pre-apply quality gates
- `DataContract` validation folds into SQLMesh contract system
- Virtual environments enable safe iteration on transform logic

### DuckLake / DuckDB — Storage

**Role**: Remains as the primary storage layer, enhanced with multi-source catalog.

**What's new**:
- Multi-source catalog: DuckLake tables per source with cross-source union views
- Feature store schema: point-in-time correct feature views for ML
- Iceberg via DuckDB: DuckDB can read/write Iceberg tables natively (future-proofing)

### Prefect — Orchestration

**Role**: Remains as the orchestrator, enhanced with dlt task wrappers and SQLMesh plan/apply.

**What's new**:
- `dlt` pipeline runs as `@task` instruments
- `sqlmesh plan` and `sqlmesh apply` as Prefect flows
- Multi-source cron deployments (Binance daily, tardis.dev hourly, Databento real-time)
- Enhanced DLQ with source-aware routing
- Event-driven triggers (new partition → automatic processing)

## Source Integration Matrix

| Source | Latency | Access Method | dlt Resource | SQLMesh Model | Tables |
|--------|---------|---------------|-------------|---------------|--------|
| Binance Archive | ~1-2 days | S3 XML listing + aria2c | `@dlt.resource` (custom S3) | `bronze_klines → silver_klines` | klines, aggTrades, fundingRate |
| Binance REST API | Real-time | SDK `rest_api.klines()` | `@dlt.resource` (incremental) | (gap-fill, same model) | klines, aggTrades, fundingRate |
| Binance WS | Real-time | SDK WS + Queue | `@dlt.resource` (append) | `silver_klines_streamed` | klines, trades |
| tardis.dev CSV | ~T+6min | S3/HTTP CSVs | `@dlt.resource` (replace) | `silver_trades`, `silver_book_l2` | trades, bookL2, liquidations, options |
| tardis.dev API | Real-time | HTTP REST/WS | `rest_api_source` | (stream to silver) | trades, bookL2 |
| Databento | Real-time | DBN binary | `@dlt.resource` (custom DBN decoder) | (bypasses Bronze, native Silver) | klines, trades, mbp, mbo |

## Silver Schema Evolution

### Unified Silver Schema (current, remains)

The existing `ts_event`/`ts_recv`/`source`/`exchange`/`symbol` schema follows DBN conventions and
is source-agnostic. This is correct and must be preserved.

**Schema additions for new sources**:

```sql
-- New columns for multi-source
ALTER TABLE silver_klines ADD COLUMN source_specific_id VARCHAR;  -- tardis.dev trade ID, Databento sequence
ALTER TABLE silver_klines ADD COLUMN dataset VARCHAR;              -- "GLBX.MDP3", "BINANCE.OHLCV"
ALTER TABLE silver_klines ADD COLUMN feed_mode VARCHAR;            -- "historical", "streaming", "batch"
```

### Cross-Source Union Views

```sql
CREATE VIEW all_klines AS
SELECT * FROM binance_archive.silver_klines
UNION ALL BY NAME
SELECT * FROM tardis_dev.silver_klines
UNION ALL BY NAME
SELECT * FROM databento.silver_klines;
```

## DataOps Requirements

| Requirement | Description | How |
|------------|-------------|-----|
| **DR-1: Data Contracts** | Schema and constraint validation on load | dlt contract modes (evolve/freeze/discard) |
| **DR-2: Lineage Tracking** | End-to-end provenance: source→bronze→silver→feature | Existing LineageTracker + SQLMesh column-level lineage |
| **DR-3: Quality Gates** | Automated checks before promoting data | SQLMesh audits (null/duplicate/outlier checks) |
| **DR-4: Freshness SLAs** | Alert when data exceeds max_stale window | Prefect SLA monitors + health_flow |
| **DR-5: Schema Evolution** | Handle upstream schema changes safely | SQLMesh virtual environments + forward-only mode |
| **DR-6: DLQ Routing** | Failed records routed to dead letter queue | Existing DLQ + dlt failed row tracking |
| **DR-7: Anomaly Detection** | Statistical outlier detection (Z-score, nulls) | Existing check_ducklake_anomalies() + SQLMesh audits |

## MLOps Requirements

| Requirement | Description | How |
|------------|-------------|-----|
| **MR-1: Point-in-Time Correctness** | No look-ahead bias in feature datasets | SQLMesh INCREMENTAL_BY_TIME_RANGE + Databento point-in-time defs |
| **MR-2: Feature Views** | Pre-computed feature tables for ML | SQLMesh models with downstream materialization |
| **MR-3: Backtesting Slices** | Time-windowed exports for backtesting | DuckDB time-range queries + Parquet export |
| **MR-4: Training Datasets** | Labeled data for supervised learning | Feature Store union views |
| **MR-5: Reproducible Builds** | Deterministic data pipelines versioned in git | SQLMesh snapshot versioning + Prefect flow versioning |

## Implementation Phases

### Phase 1: Foundation — dlt Integration (2-3 weeks)

**Goal**: Replace hand-rolled extract with dlt while keeping existing workflows as parallel path.

**Tasks**:
1. Add `dlt` to project dependencies
2. Create `@dlt.resource` wrappers for existing exchange clients:
   - `binance_klines_resource(symbol, interval, since, until)` — incremental on `open_time`
   - `binance_agg_trades_resource(symbol, since, until)` — incremental on `transact_time`
   - `binance_funding_rate_resource(symbol, since, until)` — incremental on `funding_time`
3. Create dlt pipeline factory: `build_pipeline(source_name, destination="duckdb", dataset_name="bronze")`
4. Integration test: dlt pipeline ingests Binance REST data into DuckDB
5. Verify Bronze schemas match existing format

**Success criteria**: dlt pipeline produces DuckDB tables that match existing Bronze CSV schemas.

### Phase 2: SQLMesh Transform Migration (2-3 weeks)

**Goal**: Replace Polars-based Bronze→Silver transforms with SQLMesh models.

**Tasks**:
1. Add `sqlmesh` to project dependencies
2. Initialize SQLMesh project with DuckDB backend
3. Create Bronze models that read from dlt DuckDB tables:
   ```sql
   MODEL (name bronze.klines, kind VIEW);
   SELECT * FROM duckdb.bronze.klines;
   ```
4. Create Silver models:
   ```sql
   MODEL (
     name silver.klines,
     kind INCREMENTAL_BY_TIME_RANGE (time_column ts_event),
     audits (not_null_ts_event, positive_prices)
   );
   SELECT
     ts_event, ts_recv, open, high, low, close,
     volume, quote_volume, trade_count,
     taker_buy_volume, taker_buy_quote_volume,
     source, exchange, trade_type, symbol,
     interval, data_type, ingested_at
   FROM bronze.klines
   WHERE ts_event BETWEEN @start_ds AND @end_ds;
   ```
5. Write audits that replace `check_ducklake_anomalies()`:
   ```sql
   AUDIT (name not_null_ts_event);
   SELECT * FROM silver.klines WHERE ts_event IS NULL;
   ```
6. Wire `sqlmesh plan/apply` into Prefect flows

**Key design decision**: Keep `SinkWorkflow` as parallel path for backward compatibility. Users
who prefer Polars transforms can continue using it. New users default to SQLMesh.

### Phase 3: tardis.dev Integration (1-2 weeks)

**Goal**: Ingest tick-level, multi-exchange data from tardis.dev.

**Tasks**:
1. Create `TardisDevClient` implementing `ExchangeClient` protocol
2. Create `@dlt.resource` for tardis.dev:
   - `tardis_trades_resource(exchange, symbol, date)` — daily CSV or API
   - `tardis_book_l2_resource(exchange, symbol, date)` — order book snapshots
3. Map tardis.dev schemas to existing Silver conventions:
   - `timestamp` → `ts_event` (already μs)
   - `price` → `price` (same)
   - `amount` → `size` (same)
   - `side` → `side` (same: "buy"/"sell")
4. Add tardis.dev exchange values: `tardis.dev_<exchange>` for Silver `exchange` column
5. Create SQLMesh models for tardis.dev tables

**Key advantage**: tardis.dev normalized CSVs map almost 1:1 to existing Silver schemas. The
hardest part is the symbol mapping (tardis.dev uses Binance-style symbols natively for Binance).

### Phase 4: Databento Integration (2-3 weeks)

**Goal**: Ingest institutional-grade data from Databento (CME, OPRA).

**Tasks**:
1. Create `DatabentoClient` implementing `ExchangeClient` protocol
2. Add `databento` Python SDK to dependencies
3. Create `@dlt.resource` for Databento:
   - `databento_ohlcv_resource(dataset, symbols, start, end)` — DBN-decoded data
   - `databento_mbp_resource(dataset, symbols, depth)` — market-by-price
4. Create DBN decoder shim (Rust `dbn` bindings or Python SDK built-in)
5. Map Databento schemas:
   - `ts_event` → already DBN format (direct mapping)
   - Databento data bypasses Bronze→Silver transform (native Silver format)
6. Create cross-source union views

**Key advantage**: The existing Silver layer already uses DBN conventions (`ts_event`, `ts_recv`).
Databento data is the closest to native Silver — essentially zero transformation needed.

### Phase 5: DataOps/MLOps Layer (2-3 weeks)

**Goal**: Formalize data contracts, quality gates, feature engineering, and point-in-time datasets.

**Tasks**:
1. Integrate `DataContract` validation as SQLMesh audits
2. Create Feature Store schema: `feature_views` DuckDB schema with materialized features:
   - `vwap_1h`, `volatility_1h`, `spread_avg`, `order_book_imbalance`
3. Create point-in-time correct datasets:
   - `backtest_klines` — no look-ahead bias, no retroactive adjustments
4. Create ML feature pipelines:
   - `features/lagged_prices.sql` — lagged price features
   - `features/microstructure.sql` — order book derived features
   - `features/funding.sql` — funding rate features
5. Add freshness SLAs as Prefect monitors
6. Add anomaly detection alerting (Slack/email via Prefect notifications)

**Key design decision**: Feature engineering lives in SQLMesh models, not in application code.
This ensures lineage tracking, versioning, and zero-copy env isolation for features.

### Phase 6: Skills Framework (1-2 weeks)

**Goal**: Implement the subagent skill system from the existing skills-subagents.md spec.

**Tasks**:
1. Create `skills/` directory with skill manifest schema
2. Implement `discover_symbols` skill (wraps ArchiveListSymbolsWorkflow + SourceRegistry)
3. Implement `download_partition` skill (wraps dlt pipeline run)
4. Implement `verify_partition` skill (wraps ArchiveVerifyWorkflow + SQLMesh audits)
5. Implement `transform_partition` skill (wraps SQLMesh plan/apply)
6. Implement `publish_partition` skill (wraps DuckDB + Feature Store)
7. Write CLI entry points for each skill
8. Add agent-friendly output format (JSON lines)

### Phase 7: Real-Time Streaming (future, not in scope)

**Design decision per YAGNI**: Real-time streaming (WS→DuckDB) is deferred. The current
architecture supports it (WS clients + `@dlt.resource(append)` + SQLMesh `INCREMENTAL_BY_TIME`),
but there is no user demand yet. See `docs/proposals/realtime-streaming.md` for the design.

## Architecture Decisions

### AD-1: dlt over Hand-Rolled Extract

**Decision**: Use dlt for all new source integrations. Keep existing Binance archive workflows as
parallel path.

**Rationale**: dlt provides pagination, retries, state management, schema inference, and
incremental loading as declarative config. Writing this by hand for each new source (tardis.dev,
Databento, future sources) would violate DRY.

**Cost**: dlt adds ~5MB to dependency footprint. Mature (15k+ stars, active development).

### AD-2: SQLMesh over Polars for Transform

**Decision**: Use SQLMesh for all new transform pipelines. Keep existing SinkWorkflow as parallel
path for archive data.

**Rationale**: SQLMesh provides `INCREMENTAL_BY_TIME_RANGE` (perfect for time-series market data),
automatic backfill scoping (surgical instead of full-refresh), column-level lineage, built-in
audits, and virtual environments for safe iteration. Polars transforms are harder to version,
test, and verify.

**Cost**: SQLMesh adds Python dependencies (SQLGlot, etc.). The SQLMesh project structure adds
files to the repo.

### AD-3: DuckDB/DuckLake as Primary Store

**Decision**: Keep DuckDB/DuckLake as the primary storage layer. No migration to Snowflake,
BigQuery, or ClickHouse unless scaling requirements demand it.

**Rationale**: DuckDB handles terabytes on a single machine. For a personal/team-scale crypto data
platform, it's sufficient. If multi-engine access is needed, Iceberg catalog can be added later
via DuckDB's native Iceberg support.

### AD-4: Source-Aware Catalog

**Decision**: DuckLake tables are per-source with cross-source union views, not a single table
per data type.

**Rationale**: Different sources have different latency, schema details, and quality guarantees.
A single table with `source` column would require complex reconciliation logic. Per-source tables
simplify audit, backfill, and data quality workflows. Union views provide cross-source queries.

### AD-5: Backward Compatibility

**Decision**: All existing workflows and CLI commands continue to work unchanged. New features are
opt-in.

**Rationale**: User base depends on the CLI. Breaking changes would erode trust. The new stack
(dlt, SQLMesh) coexists with existing code during the transition.

## Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| SQLMesh introduces complexity for simple transforms | Medium | Medium | Keep SinkWorkflow as parallel path; SQLMesh only for new transforms |
| dlt rest_api_source doesn't handle Binance klines array format | Medium | Low | Custom `@dlt.resource` wrapper (20 lines) instead of generic source |
| tardis.dev API rate limits | Low | Medium | dlt handles retry/backoff; batch downloads for large backfills |
| Databento Python SDK compatibility | Low | Medium | Pin SDK version; fallback to HTTP API if SDK breaks |
| Schema drift between tardis.dev and existing Silver | Medium | Low | SQLMesh virtual environments + audits catch drift before promotion |
| Prefect + dlt + SQLMesh dependency conflicts | Low | High | Pin all dependency versions; test matrix in CI |

## Success Criteria

1. **dlt pipeline** ingests Binance REST klines into DuckDB with verified schema match
2. **SQLMesh models** produce Silver tables identical to existing SinkWorkflow output
3. **tardis.dev source** ingests trades from 3+ exchanges into DuckDB
4. **Databento source** ingests CME futures OHLCV into DuckDB
5. **Cross-source union view** `all_klines` returns data from all sources
6. **SQLMesh audits** catch null prices, duplicate timestamps, date gaps before promotion
7. **Feature store** produces point-in-time correct VWAP and volatility features
8. **All existing tests pass** (280+ tests)
9. **No existing CLI commands changed**

## Next Steps

→ `/ce:plan` for structured implementation planning of Phase 1 (dlt Integration).

Requirements doc: `docs/brainstorms/2026-05-11-platform-evolution-requirements.md`

Key decisions:
- dlt replaces hand-rolled extract for new sources
- SQLMesh replaces Polars transforms for new pipelines
- DuckDB/DuckLake remains primary store
- Existing workflows preserved for backward compatibility
- tardis.dev + Databento as primary new sources
