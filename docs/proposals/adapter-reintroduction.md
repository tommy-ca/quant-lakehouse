Title: Adapter Reintroduction — Minimal DataSourceAdapter
Date: 2026-05-16

Summary
-------
We reintroduced a very small adapter layer to provide a safe, testable
extension point for multi-source ingestion. The adapter is intentionally
minimal: it exposes a DataSourceAdapter protocol, a BinanceAdapter that wraps the
existing ArchiveClient, and a SourceRegistry singleton for discovery.

Why
---
- The codebase is heavily optimized for data.binance.vision S3 semantics. Full
  multi-source support should not force a large rewrite. A small adapter
  surface preserves the existing workflows and allows gradual on-ramp for new
  sources.
- Follows SOLID: Dependency Inversion — workflows depend on a protocol, not a
  concrete ArchiveClient.
- Follows KISS/DRY/YAGNI: minimal API surface; no speculative features.

Files Added
-----------
- src/binance_datatool/adapter/protocol.py — DataSourceAdapter Protocol
- src/binance_datatool/adapter/binance.py — BinanceAdapter (wraps ArchiveClient)
- src/binance_datatool/adapter/registry.py — SourceRegistry + module-level registry

Behavior
--------
- Default registry maps "binance" → BinanceAdapter. The BinanceAdapter simply
  delegates to ArchiveClient (list_symbols, list_symbol_files).
- CLI: the `list-symbols` command now resolves `registry.get("binance")` and
  calls `adapter.list_symbols(...)`. If the registry fails, it falls back to the
  legacy ArchiveListSymbolsWorkflow for backwards compatibility.

Testing & Guardrails
--------------------
- Added tests/test_no_unapproved_legacy_imports.py to prevent reintroduction
  of imports from `workflow.legacy` across the codebase (whitelist small set).
- Existing ArchiveClient behavior remains unchanged; the adapter is a thin
  wrapper to avoid regressions.

Next Steps
----------
1. Add CoinbaseAdapter skeleton and integration tests (deferred until required).
2. Replace direct ArchiveClient instantiation in a few other CLI/workflow
   entrypoints with registry usage where appropriate.
3. Add `DataContract` runtime implementation (docs already specify it) and
   wire to VerifyWorkflow.

Engineering Rationale
---------------------
This approach minimizes risk: it avoids large refactors, keeps tests passing,
and provides a clear path for multi-source expansion without introducing
complexity until needed.
