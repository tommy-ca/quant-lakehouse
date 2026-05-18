# Formal Requirements & Specification Document

> **Note (2026-05-17):** The DuckLake integration has been stabilized and
> production-hardened. The primary catalog now uses a hybrid approach with
> `metadata.duckdb` (DuckDB-backed) to avoid environment-specific SQLite driver
> conflicts. Ingestion is now metadata-driven, using a central registry
> (`registry.symbols`) in the Lakehouse to discover and validate symbols for
> each market segment (`spot`, `um`, `cm`).
>
> Operational guidance: the dlt pipeline path is the authoritative ingestion
> flow. All transformations leverage dynamic view mapping in `get_connection()`
> for robust table resolution.

## 1. Project Overview

**Project**: `binance-datatool` — A multi-source cryptocurrency market data ingestion toolkit
**Current Scope**: Binance public archive (data.binance.vision) with support for spot, USD-M futures, and COIN-M futures
**Target Scope**: Multi-exchange support (Coinbase, Kraken, Bybit, etc.) with explicit DataOps and MLOps concerns
**Principles**: SOLID, KISS, DRY, YAGNI with specification-driven development and TDD

---

## 2. Functional Requirements

### 2.1 Core Data Operations

| Requirement | Description | Priority | Status |
|---|---|---|---|
| **FR-1: List Symbols** | Query a data source for all available trading symbols, with optional filtering (quote asset, leverage, stablecoins, contract type) | HIGH | ✅ Implemented |
| **FR-2: List Files** | Query a data source for available data files for one or more symbols, respecting partition and data type | HIGH | ✅ Implemented |
| **FR-3: Download Files** | Fetch data files from source using diff-based sync (only new/updated files), with resumable downloads and retry logic | HIGH | ✅ Implemented |
| **FR-4: Verify Integrity** | Validate downloaded files against SHA256 checksums, with marker caching to avoid re-verification | HIGH | ✅ Implemented |
| **FR-5: Multi-Source Support** | Enable data ingestion from multiple exchanges behind a common adapter interface | HIGH | ✅ Implemented |
| **FR-6: Data Contracts** | Define and validate schema/structure constraints on ingested data | MEDIUM | ✅ Implemented |
| **FR-7: Lineage Tracking** | Record data provenance: source, partition, transformation steps, validation status | MEDIUM | ✅ Implemented |
| **FR-8: CLI Composition** | Support stdin/stdout piping to compose commands into multi-step workflows | HIGH | ✅ Implemented |
| **FR-9: Metadata Registry** | Centralized venue and symbol registry in the Lakehouse for dynamic discovery | HIGH | ✅ Implemented |

---

## 10. Next Steps & Roadmap

... [previous phases same] ...

### Phase 43: DuckLake Hardening & Metadata Registry (2026-05-17)

**Goal**: Resolve environmental driver failures, stabilize the catalog, and implement
metadata-driven ingestion.

**Changes**:
- ✅ **Stabilized DuckLake**: Migrated from SQLite-backed to DuckDB-backed catalog
  (`metadata.duckdb`) to resolve `PRAGMA WAL` driver conflicts.
- ✅ **Metadata Registry**: Implemented `src/binance_datatool/common/metadata_registry.py`
  to synchronize Binance venues and symbols into `registry.symbols` table.
- ✅ **Dynamic View Mapping**: Updated `src/binance_datatool/storage/duckdb.py` to
  automatically map on-disk Parquet files to DuckDB views, bypassing fragile
  `ATTACH` logic.
- ✅ **Unified Tables**: Refactored `src/binance_datatool/dlt/sources.py` to ingest all
  symbols into shared tables (`klines`, `agg_trades`, etc.) instead of per-symbol tables.
- ✅ **Schema Robustness**: Patched `src/binance_datatool/transforms/funding_rate.py`
  to gracefully handle missing columns (e.g. `mark_price`) during schema evolution.
- ✅ **Production E2E Validation**: Successfully backfilled 30 days of 1m klines for
  BTC and ETH across all 3 market segments (`spot`, `um`, `cm`) with full
  transformation and health audit.

**Current baseline**: 281 unit tests passing, 14 E2E integration tests passing,
full 30-day production scale validation passing. Lint clean, format clean.

---

**Document Version**: 2.0
**Last Updated**: 2026-05-17
**Maintainer**: Team
**Status**: Production-ready. 100% E2E coverage for primary data types.
