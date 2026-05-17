---
date: 2026-05-17
topic: orchestration-hardening
status: active
---

# Orchestration Hardening Requirements

## 1. Problem Definition
During the execution of Phase 2 of the Migration Plan, a code review identified two critical integration gaps between the new `dlt` resources, the `asyncio` SDK clients, and the `Prefect` orchestration layer. These issues prevent the pipeline from reliably processing multiple symbols concurrently.

### 1.1 P0 - Event Loop Conflict
*   **The Issue:** `dlt` resources (e.g., `binance_klines.py`, `binance_agg_trades.py`) are synchronous generators. To consume data from the underlying async Binance SDK, they currently use `asyncio.run()`.
*   **The Failure:** When these `dlt` resources are invoked inside a Prefect `@task` (which runs within its own managed event loop/thread pool), `asyncio.run()` throws a `RuntimeError` ("asyncio.run() cannot be called from a running event loop").

### 1.2 P1 - DuckDB Concurrency Gap
*   **The Issue:** `prefect_flows.py` orchestrates parallel execution across symbols. While the final `sink_silver` write to DuckLake is protected by a `ducklake-writer` concurrency guard, the upstream `dlt` extraction tasks (`run_dlt_source`, `run_dlt_agg_trades`, etc.) write to `catalog.duckdb` (the bronze layer) without any global concurrency protection.
*   **The Failure:** DuckDB allows only one writer at a time. Parallel `dlt` extractions will encounter "Database is locked" `IOException`s.

## 2. Success Criteria
*   **C1:** `dlt` resources must successfully fetch data from the async SDK without raising `RuntimeError`s, regardless of the calling context (standalone CLI, Prefect Flow, or embedded).
*   **C2:** Prefect flows (`dlt_historical_pipeline`, `historical_pipeline`) must successfully execute parallel extraction tasks (`map()`) across multiple symbols without raising DuckDB concurrency errors.
*   **C3:** The solution must not sacrifice the ability to process multiple symbols concurrently at the extraction phase (i.e., we should not force the entire pipeline to be strictly sequential).

## 3. Explored Approaches

### 3.1 Resolving the Event Loop Conflict

#### Approach A: Persistent Background Event Loop (Recommended)
Create a single, persistent background thread that runs an `asyncio` event loop. All coroutines from synchronous `dlt` resources are dispatched to this loop using `asyncio.run_coroutine_threadsafe`.
*   **Pros:** Isolates the event loop from Prefect safely. Extremely low overhead since threads aren't created per-call.
*   **Cons:** Requires managing the lifecycle of the background thread.

#### Approach B: Spawning Threads Per Call
Wrap each async SDK call in a helper that spawns a new thread, runs `asyncio.run()`, and exits.
*   **Pros:** Isolates the event loop.
*   **Cons:** Massive thread creation overhead inside iterative `dlt` extraction loops; highly inefficient.

### 3.2 Resolving the DuckDB Concurrency Gap

#### Approach A: Separate Parallel Extract and Serial Load (Recommended)
Configure `dlt` to write extracted data to the filesystem (e.g., Parquet files) without accessing `catalog.duckdb` directly during extraction. Then, add a separate, strictly sequential Prefect task to load all generated files into DuckDB.
*   **Pros:** Retains full parallel network extraction. Only the actual database writes are serialized.
*   **Cons:** Requires changes to the `dlt` destination configuration.

#### Approach B: Extend Concurrency Guard Over Entire Run
Apply the `"ducklake-writer"` guard to all `run_dlt_*` extraction tasks in `prefect_flows.py`.
*   **Pros:** Simple.
*   **Cons:** Holds a global database lock during slow network I/O, forcing all extractions to be completely sequential and destroying performance.

## 4. Key Decisions
*   **Event Loop**: Adopt **Approach 3.1A**. We will create a `sync_run` utility in `binance_datatool.common` backed by a persistent background event loop thread.
*   **Concurrency**: Adopt **Approach 3.2A**. We will decouple `dlt` extraction from DuckDB loading, allowing parallel network I/O and serialized database writes.

## 5. Scope Boundaries
*   We will only address the concurrency issues related to DuckDB writes.
*   We will not attempt to rewrite the Binance SDK to be synchronous.
*   We will not implement distributed locking (e.g., Redis) as the pipeline is designed for a single-node deployment (Local DuckLake).
