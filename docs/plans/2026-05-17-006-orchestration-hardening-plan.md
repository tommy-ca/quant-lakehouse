---
date: 2026-05-17
topic: orchestration-hardening
status: proposal
---

# Plan: Orchestration Hardening

## Overview
This plan implements the requirements defined in `docs/brainstorms/2026-05-17-orchestration-hardening-requirements.md` to resolve the P0 Event Loop Conflict and P1 DuckDB Concurrency Gap identified during the Phase 2 migration review.

## Implementation Units

### [U1] Create `sync_run` Utility
- **Goal:** Provide a safe mechanism to run `asyncio` coroutines from synchronous threads without conflicting with existing event loops.
- **Files:**
  - `src/binance_datatool/common/async_utils.py` (Create)
  - `tests/test_async_utils.py` (Create)
- **Approach:**
  Implement a function `sync_run(coro)` that:
  1. Utilizes a persistent background daemon thread running an `asyncio` event loop.
  2. Submits `coro` to that loop using `asyncio.run_coroutine_threadsafe`.
  3. Returns the result or raises the exception back to the caller.
  4. Automatically manages the lifecycle of the background thread.
- **Verification:** `test_async_utils.py` proves `sync_run` can be called from within a running `asyncio` event loop without raising a `RuntimeError` and performs efficiently.

### [U2] Migrate `dlt` Resources to `sync_run`
- **Goal:** Remove all unsafe `asyncio.run()` calls from `dlt` resources.
- **Files:**
  - `src/binance_datatool/dlt/resources/binance_klines.py`
  - `src/binance_datatool/dlt/resources/binance_agg_trades.py`
  - `src/binance_datatool/dlt/resources/binance_funding.py`
  - `src/binance_datatool/dlt/resources/binance_archive.py`
  - `src/binance_datatool/dlt/resources/binance_metadata.py`
- **Approach:** Import `sync_run` and replace `asyncio.run(...)` calls.
- **Verification:** Unit tests for resources still pass.

### [U3] Separate `dlt` Extraction from DuckDB Loading
- **Goal:** Prevent concurrent writes to `catalog.duckdb` without serializing the network I/O during multi-symbol backfills.
- **Files:**
  - `src/binance_datatool/workflow/prefect_tasks/extract.py`
  - `src/binance_datatool/workflow/prefect_flows.py`
- **Approach:**
  - Modify `_run_dlt` in `extract.py` to only run the extraction step of the dlt pipeline (e.g., using `pipeline.extract()`).
  - Introduce a new sequential step in `prefect_flows.py` to load the extracted data into DuckDB under the `ducklake-writer` guard (e.g., `pipeline.normalize()` and `pipeline.load()`).
- **Verification:** An integration test can run `dlt_historical_pipeline` with 3 symbols and confirm parallel network extraction and no "Database is locked" errors occur during the load phase.
## Scope Boundaries
- **Out of Scope:** Optimizing memory consumption in `BinanceRestClientBase.fetch_ohlcv` (P2 finding). This will be handled in a separate performance optimization pass.
- **Out of Scope:** Refactoring the `dlt` schema configuration to completely remove legacy schemas (P2 finding).

## Review Checklist
- [ ] `RuntimeError` no longer occurs when running `dlt` within Prefect.
- [ ] Concurrency errors are avoided during parallel symbol extraction.
