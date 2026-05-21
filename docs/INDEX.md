# Documentation Index & Project Status

> **Note (2026-05-16):** Several components were removed during Phase 35
> (YAGNI cleanup): `datacontract.py`, `validation/models.py`, `IcebergCatalog`,
> analytics views. The `adapter/` package was removed in Phase 35 and
> **re-introduced in Phase 42** as a minimal package
> (`src/binance_datatool/adapter/`) with `DataSourceAdapter` protocol,
> `BinanceAdapter`, and `SourceRegistry`. The `src/binance_datatool/skills/`
> directory does not exist (skills not implemented).
>
> For the **current** architecture, see:
> - `architecture.md` — current package tree and layer design
> - `requirements.md` (§Phase 42) — recent adapter re-introduction
> - `AGENTS.md` — stack architecture table, validation layer, and working model

## Overview

This index provides a complete picture of the binance-datatool project after comprehensive requirements, specification, and architecture review. It documents the current state, completed work, and clear path forward for implementation.

---

## 📚 Core Documentation (Read in Order)

### 1. **requirements.md** ⭐ START HERE
   - **Purpose**: Formal functional and non-functional requirements
   - **Covers**: Use cases, actors, architecture requirements, data models, pre-merge audit checklist
   - **Length**: ~400 lines
   - **Key Sections**:
     - Functional requirements (FR-1 through FR-8)
     - Non-functional requirements (NFR-1 through NFR-6)
     - Four-layer and six-layer architecture diagrams
     - Data model and contracts

### 2. **architecture.md**
   - **Purpose**: System design and layering
   - **Covers**: Package tree, four-layer design, data flow, extension roadmap
   - **Length**: ~130 lines
   - **Action Items**: Add Mermaid sequence diagrams (Phase 5)

### 3. **data-flows.md** ⭐ DETAILED FLOWS
   - **Purpose**: Step-by-step data and code flows for all commands
   - **Covers**: ASCII sequence diagrams, state transitions, error handling, multi-source adapter flow
   - **Length**: ~700 lines
   - **Key Sections**:
     - list-symbols flow (with state machine)
     - list-files flow (concurrent, resilient)
     - download flow (diff + resume + verify markers)
     - verify flow (parallel hashing)
     - multi-source adapter protocol

### 4. **specs-driven-development.md**
   - **Purpose**: Development process and standards
   - **Covers**: Spec template, TDD workflow, test structure, audit checklist, SOLID principles
   - **Length**: ~250 lines
   - **Key Sections**:
     - Spec template (use for every feature)
     - Pre-merge audit checklist (8 sections, 30+ checkboxes)
     - Test layer classification (unit, integration, E2E)

### 5. **skills-subagents.md** ⭐ AGENT INTERFACE
   - **Purpose**: Formal specification for AI agents and scripts
   - **Covers**: 5 core skills with input/output schemas, error modes, test cases
   - **Length**: ~550 lines
   - **Skills Defined**:
     - discover-symbols (list symbols with filters)
     - list-files (list data files per symbol)
     - download-partition (download with diff + resume)
     - verify-partition (hash and verify files)
     - validate-contract (schema validation + lineage)

### 6. **AGENTS.md** ⭐ WORKING REFERENCE
    - **Purpose**: Complete operational reference for the current codebase
    - **Covers**: Stack architecture, CLI commands, Prefect flows, data flow
      diagrams, validation layer (Pydantic + Pandera), Silver schemas,
      E2E correctness tests, Phase 42 consolidation plan
    - **Key Sections**:
      - dlt + Polars + Pandera + DuckLake stack table
      - Data type coverage matrix
      - Silver schema column counts (19 klines, 18 aggTrades, 12 fundingRate)
      - Validation layer: per-record (Pydantic) and per-DataFrame (Pandera)
      - Prefect flow task graph
### 8. **UNIVERSE_SPEC.md** ⭐ NEW
   - **Purpose**: Specification for institutional-grade tradable universes
   - **Covers**: Liquidity scoring, institutional risk filtering (memes, leverage),
     point-in-time construction logic, and Gold layer data contracts

### 9. **DATA_CONTRACT_SPEC.md** ⭐ NEW
   - **Purpose**: Formal definition of the Dual-Layer Validation Contract
   - **Covers**: Pydantic per-record ingest, Pandera per-DataFrame consistency,
     and native DuckLake partition specifications.

### 10. **REPRODUCIBILITY_SPEC.md** ⭐ NEW
   - **Purpose**: Scientific reproducibility and dataset versioning guide
   - **Covers**: DVC-backed building, Hugging Face Hub distribution, and
     manifest-driven publishing (manifest.json, dvc.lock).

### 11. **SDK_PLAN.md** ⭐ NEW
   - **Purpose**: Roadmap for the `binance-datatool-sdk`
   - **Covers**: Hugging Face integration, DuckLake consumption patterns, and planned API.

### 12. **audit.md**
   - **Purpose**: Findings from code review
   - **Covers**: Current implementation, risks, priorities, recommendations
   - **Priorities**:
     1. Data contracts + validation (✅ DONE: dlt/models.py)
     2. Lineage tracking (✅ DONE: dlt state)
     3. Multi-source adapter (✅ DONE: adapter/binance.py)
     4. Point-in-Time Universe (✅ DONE: universe/ module)

---

## 🔧 Code & Implementation (Current State)

### Universe Module (✅ PHASE 44)
- **Directory**: `src/binance_datatool/universe/`
- **Builder**: `UniverseBuilder` — builds Top-50 universes with institutional filters.
- **Rates**: `RateProvider` — dynamic USD normalization with historical support.
- **Pipeline**: `gold_pipeline.py` — materializes `gold.daily_universe_stats`.

### Adapter Package (✅ PHASE 42)
...
- **Directory**: `src/binance_datatool/adapter/`
- **Files**: `protocol.py` (DataSourceAdapter), `binance.py` (BinanceAdapter),
  `registry.py` (SourceRegistry), `__init__.py`
- **Protocol**: `DataSourceAdapter` — 3 methods: `list_symbols`, `list_symbol_files`, `get_name`
- **Purpose**: Thin adapter layer for multi-source support (SOLID: Dependency Inversion).
  `BinanceAdapter` wraps `ArchiveClient` (DRY, no rewrite).

### Validation Layer (✅ INTEGRATED)
- **Pydantic** (`dlt/models.py`): 8 models for per-record validation at dlt ingest.
  `RawKlineModel`, `RawAggTradeModel`, `RawFundingRateModel` (all VARCHAR for bronze).
  `KlineModel`, `AggTradeModel`, `FundingRateModel`, `VenueModel`, `SymbolMetaModel`.
- **Pandera** (`validation/schemas.py`): 6 schemas for per-DataFrame validation at
  Polars transform boundary. `BronzeKlinesSchema`, `BronzeAggTradesSchema`,
  `BronzeFundingRateSchema` (bronze); `SilverKlinesSchema`, `AggTradesSilverSchema`,
  `FundingRateSilverSchema` (silver, with `ts_date: pl.Date`).
- Cross-column checks (`high >= low`) enforced in both layers.

### Not Implemented
- `src/binance_datatool/datacontract.py` — **removed** (Phase 35 YAGNI)
- `src/binance_datatool/skills/` — **not implemented** (no skills/ directory)
- `IcebergCatalog` — **removed** (Phase 36 YAGNI); DuckLake is primary storage
- Analytics views (`daily_ohlcv`, `latest_klines`, `stale_symbols`) — **removed**

---

## 📊 Project Status Matrix

| Component | Status | File | Notes |
|-----------|--------|------|-------|
| **DataSourceAdapter** | ✅ Complete | adapter/protocol.py | 3 methods |
| **BinanceAdapter** | ✅ Complete | adapter/binance.py | Wraps ArchiveClient |
| **SourceRegistry** | ✅ Complete | adapter/registry.py | Module-level singleton |
| **Pydantic models** | ✅ Complete | dlt/models.py | 8 models (Raw*, Kline*, etc.) |
| **Pandera schemas** | ✅ Complete | validation/schemas.py | 6 schemas (Bronze*Silver*) |
| **Bronze→Silver transforms** | ✅ Complete | transforms/ | klines, agg_trades, funding_rate |
| **Exchange Clients** | ✅ Complete | exchange/*.py | SDK-backed Spot/UM/CM REST + WS |
| **DuckLake storage** | ✅ Complete | dlt/destinations.py | dlt destination by default |
| **Prefect orchestration** | ✅ Complete | workflow/prefect_flows.py | 4 flows, concurrency guards |
| **Metadata tables** | ✅ Complete | workflow/metadata.py | venues.parquet, symbols.parquet |
| **CLI commands** | ✅ Complete | cli/archive.py | 7 commands (list-symbols→refresh-metadata) |
| **DataContract** | ❌ Removed | — | Phase 35 YAGNI cleanup |
| **Skills** | ⏳ Not impl. | — | No skills/ directory |
| **IcebergCatalog** | ❌ Removed | — | Phase 36 YAGNI cleanup |

---

## 🎯 Implementation Roadmap

### Phase 1: Foundation & Specification (✅ COMPLETE)
- [x] Architecture overview (architecture.md)
- [x] Formal requirements (requirements.md)
- [x] Detailed data flows (data-flows.md)
- [x] DataContract implementation & tests
- [x] Skills specification (skills-subagents.md)
- [x] Specs-driven development guide (specs-driven-development.md)
- [x] Implementation guide (implementation-guide.md)

### Phase 6: Exchange SDK Migration (✅ COMPLETE)
- [x] Migrated REST/WS clients to official Binance SDK
- [x] Extended ExchangeClient protocol (aggTrades, fundingRate)
- [x] Backward compatibility via BinanceRestClient/BinanceWsClient aliases
- [x] Auto gap detection + health monitoring workflows

### Phase 7: Silver Layer & DuckLake Catalog (✅ COMPLETE)
- [x] Polars-based Bronze→Silver transform (klines, aggTrades, fundingRate)
- [x] Silver schemas following Databento DBN (ts_event, ts_recv) + tardis.dev
- [x] DuckLake v1.0 native tables with partitioning (exchange, data-type, symbol, interval, date)
- [x] Self-describing catalog paths: exchange=binance-spot/data-type=klines/symbol=BTCUSDT/interval=1h/date=N/data.parquet
- [x] Venue/symbol metadata tables (venues.parquet, symbols.parquet)
- [x] CLI: gap-fill, health, sink, refresh-metadata commands
- [x] Pandera validation at Polars boundary (6 schemas)
- [x] Pydantic models at dlt ingest boundary (8 models, all VARCHAR for bronze)

### Phase 42: Legacy Consolidation (✅ COMPLETE — 2026-05-16)
- [x] Re-introduced minimal adapter package (DataSourceAdapter + BinanceAdapter + SourceRegistry)
- [x] Wired CLI list-symbols to registry (`--source adapter` opt-in)
- [x] Guard test for legacy workflow.legacy imports (pre-commit + CI)
- [x] Deprecated notes on legacy wrappers (gap_fill, sink, prefect_flows)
- [x] Docs sweep (AGENTS.md, extending.md, data-flows.md, implementation-guide.md)
- [x] Pydantic ↔ Pandera alignment audit (constraints consistent)

### Phase 8: Planned Future Work (⏳)
- [ ] CoinbaseAdapter or other exchange adapters (demand-driven)
- [ ] Prefect flows for multi-symbol fan-out (historical_pipeline bulk backfill)
- [ ] Mermaid diagrams for documentation
- [ ] SQLMesh INCREMENTAL_BY_TIME_RANGE models (optional path alongside Polars)

## 🧪 Testing Strategy

### Test Layers

| Layer | Scope | Speed | When |
|-------|-------|-------|------|
| **Unit** | Single class/function; fakes for I/O | <100ms | Always (block merge if fail) |
| **Integration** | Workflows + real I/O (may use fixtures); marked @pytest.mark.integration | <5s | Before merge (can skip in CI) |
| **E2E** | Full CLI + real Binance API | >10s | Pre-release only |

### Current Test Status

```
✅ Validation layer: 21 passing (Pandera schemas + Pydantic models + consistency)
✅ CLI tests: 32 passing, 2 skipped (integration)
✅ Adapter guard: 1 passing (legacy imports whitelist)
✅ Archive workflow tests: 36 passing
✅ All tests combined: 277 passing, 8 skipped (unit)
---
Run: uv run pytest tests/ -q --ignore=tests/test_e2e_correctness.py
```

# Running Tests

```bash
# All unit tests (no network)
uv run pytest tests/ -q --ignore=tests/test_e2e_correctness.py

# Specific layer
uv run pytest tests/test_validation.py -v

# With coverage
uv run pytest --cov=binance_datatool --cov-report=html

# Integration tests (requires network)
uv run pytest tests/ --run-integration

# Linting
uv run ruff check .
uv run ruff format .
```

---

## 📖 How to Use This Documentation

### For Implementers

1. **Read** `AGENTS.md` (understand the current working model)
2. **Read** `data-flows.md` (understand the "how")
3. **Follow** `extending.md` (add schemas, adapters, workflows)
4. **Check** `specs-driven-development.md` before PR (audit checklist)

### For Code Reviewers

- Use `specs-driven-development.md` pre-merge audit checklist
- Verify PR implements one of the specifications
- Run `uv run pytest` to check all tests pass
- Run `uv run ruff check .` for linting

### For AI Agents / Subagents

- Reference `AGENTS.md` for the current architecture and working model
- Import from `binance_datatool.adapter`, `binance_datatool.transforms`,
  `binance_datatool.validation`, `binance_datatool.workflow.prefect_tasks`
- Follow the adapter protocol (`DataSourceAdapter`) for multi-source extensions
- Example adapter usage:
  ```python
  from binance_datatool.adapter.registry import registry

  adapter = registry.get("binance")  # returns BinanceAdapter
  symbols = await adapter.list_symbols(trade_type, data_freq, data_type)
  ```
- For CLI commands, see `uv run binance-datatool --help`

---

## 🏗️ Architecture at a Glance

### Current Stack (dlt + Polars + Pandera + DuckLake)

```
CLI Layer (Typer commands) ───────────────────────────────────────────────┐
  list-symbols, list-files, download, verify, gap-fill,                  │
  health, sink, refresh-metadata                                           │
  ↓                                                                        │
Workflow / Prefect Layer ────────────────────────────────────────────────┤
  ArchiveListSymbolsWorkflow, GapFillWorkflow, HealthCheckWorkflow,      │
  SinkWorkflow, MetadataWorkflow, Prefect flows (historical_pipeline)  │
  ↓                                                                        │
Adapter Layer ───────────────────────────────────────────────────────────┤
  DataSourceAdapter protocol + BinanceAdapter (wraps ArchiveClient)      │
  SourceRegistry for multi-source discovery                               │
  ↓                                                                        │
Data Source Layer ───────────────────────────────────────────────────────┤
  Archive (data.binance.vision S3) | REST API (Binance SDK) | WS Stream   │
  ↓                                                                        │
DLT Extract + Load ───────────────────────────────────────────────────────┤
  bronze.klines / bronze.agg_trades / bronze.funding_rate (VARCHAR)       │
  Per-record Pydantic validation (RawKlineModel, RawAggTradeModel, ...)  │
  ↓                                                                        │
Polars Transform ─────────────────────────────────────────────────────────┤
  bronze_*_to_silver() with timestamp normalization, μs auto-detection   │
  ↓ Pandera validation at boundary                                        │
DuckLake / DuckDB Silver ────────────────────────────────────────────────┤
  silver.klines (19 cols) | silver.agg_trades (18 cols)                  │
  silver.funding_rate (12 cols)                                           │
```

**Key Addition**: Adapter layer abstracts source-specific behavior; enables multi-source support.

---

## 📋 Pre-Merge Checklist (Audit Checklist)

Every PR must satisfy:

```
[ ] Requirements
  [ ] Addresses explicit requirement (FR-X, NFR-X)
  [ ] Scope is small and focused

[ ] Specification
  [ ] Spec written using template from specs-driven-development.md
  [ ] All error cases documented

[ ] Tests (TDD)
  [ ] Unit tests written FIRST (failing)
  [ ] Implementation added (tests pass)
  [ ] Integration tests for I/O
  [ ] Edge cases covered

[ ] Design (SOLID / KISS / DRY / YAGNI)
  [ ] Single Responsibility
  [ ] Open/Closed
  [ ] Liskov Substitution
  [ ] Interface Segregation
  [ ] Dependency Inversion

[ ] Documentation
  [ ] Docstrings added (Google style)
  [ ] README updated (if user-facing)
  [ ] Architecture docs updated (if structural change)

[ ] Code Quality
  [ ] Tests pass: uv run pytest
  [ ] Linting passes: uv run ruff check .
  [ ] No hard-coded secrets

[ ] Ready
  [ ] All boxes checked
  [ ] Approval from 1+ reviewer
  [ ] CI pipeline green
```

Full checklist in `docs/specs-driven-development.md`.

---

## 🔗 Quick Links

| Document | Purpose | Length |
|----------|---------|--------|
| requirements.md | Formal specs & audit checklist | 400 lines |
| data-flows.md | Step-by-step command flows | 700 lines |
| architecture.md | System design & layers | 130 lines |
| specs-driven-development.md | Development process | 250 lines |
| skills-subagents.md | Agent API specs | 550 lines |
| implementation-guide.md | Build roadmap | 400 lines |
| audit.md | Code review findings | 80 lines |

---

## 💡 Key Design Principles

1. **SOLID** - Single Responsibility, Open/Closed, Liskov, Interface Segregation, Inversion of Control
2. **KISS** - Keep it simple; avoid premature abstraction
3. **DRY** - Don't repeat yourself; reuse shared logic
4. **YAGNI** - You ain't gonna need it; no speculative features
5. **TDD** - Tests first; write failing test before code
6. **Resilient** - Partial failures don't abort entire workflow
7. **Atomic** - Files written completely or not at all
8. **Observable** - Logs, metrics, lineage for debugging

---

## 🎓 Learning Path (For New Contributors)

1. Read `AGENTS.md` (15 min) — understand the stack architecture and working model
2. Read `architecture.md` (10 min) — understand the package structure
3. Read `data-flows.md` (20 min) — understand how data moves through the system
4. Review `transforms/klines.py` + `validation/schemas.py` (15 min) — see Polars + Pandera in action
5. Follow `extending.md` to add a new Pandera schema or adapter method (30 min)
6. Submit PR with audit checklist (see `specs-driven-development.md`)

---

## 📞 Questions & Clarifications

If unclear on:

- **Specification**: Check `skills-subagents.md` or spec template in `specs-driven-development.md`
- **Architecture**: Check `data-flows.md` or architecture diagram in `requirements.md`
- **Code examples**: Check `implementation-guide.md` or existing tests (`test_datacontract.py`, `test_source_registry.py`)
- **Development process**: Check `specs-driven-development.md` pre-merge checklist
- **Code examples**: Check `AGENTS.md`, `extending.md`, or existing tests (`tests/test_validation.py`)
- **Development process**: Check `specs-driven-development.md` pre-merge checklist
- **Next steps**: Check `tasks.md` Phase 42 and AGENTS.md

---

## 📌 Project Status

### Complete

- [x] Adapter package re-introduced (Phase 42): DataSourceAdapter + BinanceAdapter + SourceRegistry
- [x] Pydantic ↔ Pandera alignment audit (Phase 42): constraints consistent, 21 validation tests
- [x] Legacy wrappers annotated (Phase 42): gap_fill, sink, prefect_flows
- [x] Guard test for legacy imports (Phase 42): pre-commit hook
- [x] CLI `--source adapter` opt-in path wired for list-symbols
- [x] Docs sweep complete (Phase 42): AGENTS.md, extending.md, INDEX.md, implementation-guide.md
- [x] Exchange SDK migration (Phase 8): official binance-sdk-spot/derivatives-*
- [x] Silver layer: Bronze→Silver transform + Polars + DuckLake
- [x] Auto gap detection + health check
- [x] Venue/symbol metadata (venues.parquet, symbols.parquet)
- [x] CLI: 7 commands (list-symbols → refresh-metadata)
- [x] Tests: 277 passing, 8 skipped (unit)

### Open Items

- [ ] Remove legacy wrappers after 2-week zero-usage confirmation (Phase 42 removal plan)
- [ ] Wire remaining CLI commands to `--source adapter` path
- [ ] Prefect bulk backfill validation in staging
- [ ] SQLMesh INCREMENTAL_BY_TIME_RANGE models (optional, alongside Polars)
- [ ] Mermaid diagrams for documentation

---

## ✨ Summary

**Status**: Medallion-Native Lakehouse architecture finalized and validated. All tiers (Registry, Bronze, Silver, Gold) are natively managed by DuckLake with optimized partitioning. High-fidelity metadata live on Hugging Face.

**Impact**: This work enables:
- Institutional-grade datasets: Top 50 universe with zero survivorship bias.
- ACID-compliant storage: Native DuckLake management of partitioned Parquet.
- Industry alignment: Metadata standardized with DBN/Tardis schemas.
- Scientific reproducibility: DVC-backed build state with DBN/Tardis views.
- Zero-copy research: Direct attachment of HF datasets via DuckDB.

**Next Step**: Implementation of the `binance-datatool-sdk` for researcher consumption.

---

**Document Version**: 3.0
**Last Updated**: 2026-05-21
**Maintainer**: Team
**Status**: Production-ready. Gold-standard platform for institutional crypto research.
