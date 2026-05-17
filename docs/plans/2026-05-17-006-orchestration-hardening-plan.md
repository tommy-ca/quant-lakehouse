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
  1. Spawns a new daemon thread.
  2. Runs `asyncio.run(coro)` inside that thread.
  3. Returns the result or raises the exception back to the caller.
- **Verification:** `test_async_utils.py` proves `sync_run` can be called from within a running `asyncio` event loop without raising a `RuntimeError`.

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

### [U3] Extend Concurrency Guards in Prefect Flows
- **Goal:** Prevent concurrent writes to `catalog.duckdb` during parallel multi-symbol backfills.
- **Files:** `src/binance_datatool/workflow/prefect_tasks/extract.py`
- **Approach:**
  - The DuckDB destination creates tables and writes data during the pipeline run.
  - Wrap the `dlt` pipeline run invocation (specifically `_run_dlt`) with the `ducklake-writer` concurrency guard.
- **Verification:** An integration test can run `dlt_historical_pipeline` with 3 symbols and confirm no "Database is locked" errors occur.

## Scope Boundaries
- **Out of Scope:** Optimizing memory consumption in `BinanceRestClientBase.fetch_ohlcv` (P2 finding). This will be handled in a separate performance optimization pass.
- **Out of Scope:** Refactoring the `dlt` schema configuration to completely remove legacy schemas (P2 finding).

## Review Checklist
- [ ] `RuntimeError` no longer occurs when running `dlt` within Prefect.
- [ ] Concurrency errors are avoided during parallel symbol extraction.
