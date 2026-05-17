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

#### Approach A: Dedicated Thread for Async Loop (Recommended)
Wrap the async SDK calls in a synchronous helper that runs the coroutine in a separate, dedicated thread.
*   **Pros:** Isolates the event loop entirely from Prefect's environment. Safe for any caller.
*   **Cons:** Slight overhead of thread creation.

#### Approach B: Detect Existing Loop
Attempt to use `asyncio.get_running_loop()`. If a loop exists, use `loop.run_until_complete()`.
*   **Pros:** Avoids thread overhead.
*   **Cons:** `run_until_complete()` is generally unsafe to call on a loop that is already running (it blocks the loop).

### 3.2 Resolving the DuckDB Concurrency Gap

#### Approach A: Extend the Concurrency Guard (Recommended)
Apply the existing `prefect.concurrency.sync` guard (`"ducklake-writer"`) to all `run_dlt_*` extraction tasks in `prefect_flows.py`.
*   **Pros:** Uses Prefect's native concurrency limits. Simple to implement. Guarantees safety.
*   **Cons:** Forces all database writes (both Bronze dlt extraction and Silver transform writes) to be strictly sequential. However, the actual network fetching inside `dlt` is fast, so the bottleneck is acceptable.

#### Approach B: In-Memory / Ephemeral Bronze Catalogs
Have `dlt` write to temporary, per-task DuckDB files or in-memory catalogs, then merge them in a sequential step.
*   **Pros:** Maximizes parallelism.
*   **Cons:** Extremely complex implementation. Breaks the simplicity of the Bronze layer pattern. High risk of orphaned files.

## 4. Key Decisions
*   **Event Loop**: Adopt **Approach 3.1A**. We will create a `sync_run` utility in `binance_datatool.common` to safely execute coroutines in a background thread and use it in all `dlt` resources instead of `asyncio.run()`.
*   **Concurrency**: Adopt **Approach 3.2A**. We will wrap the execution of all `run_dlt_*` tasks in `prefect_flows.py` with the `"ducklake-writer"` concurrency guard.

## 5. Scope Boundaries
*   We will only address the concurrency issues related to DuckDB writes.
*   We will not attempt to rewrite the Binance SDK to be synchronous.
*   We will not implement distributed locking (e.g., Redis) as the pipeline is designed for a single-node deployment (Local DuckLake).
