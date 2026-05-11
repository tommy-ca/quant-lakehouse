---
date: 2026-05-11
topic: migration-plan
status: proposal
---

# Migration Plan: Complete Transition to New Stack

## Current State

| Layer | Legacy (3,367 LOC) | New Stack (2,500+ LOC) | Status |
|-------|-------------------|----------------------|--------|
| CLI | `cli/archive.py` imports legacy workflows | dlt sources available but not wired to CLI | ⏹ Shared |
| Download | `download.py` (226) — aria2 S3 download | `binance_archive.py` — dlt S3 download + parse | ✅ Parallel |
| Verify | `verify.py` (256) — SHA256 checksums | dlt doesn't verify (TLS integrity) | 🟡 Different |
| Gap-fill | `gap_fill.py` (468) — detect + REST fetch | `binance.py` + `gap_detection.py` — dlt REST + DuckDB gap scan | ✅ Parallel |
| Sink | `sink.py` (621) — Bronze→Silver→DuckLake | `transforms/*.py` + `workflow/db.py` — Polars + Pandera | ✅ Parallel |
| Health | `health_check.py` (353) — archive + DuckLake | `check_ducklake_anomalies()` shared; archive check replaced by dlt | ⏹ Shared |
| Metadata | `metadata.py` (234) — venues/symbols | `binance_metadata.py` — dlt sources | ✅ Parallel |
| Catalog | `catalog.py` (365) — DuckLakeCatalog | `dlt.destinations.ducklake()` — auto-managed | ✅ Replaced |
| Contracts | `datacontract.py` (443) — never used | `schema_contract="freeze"` on all dlt resources | ✅ Replaced |
| Lineage | `lineage.py` (401) — in-memory, manual export | dlt pipeline state (auto-synced) | ✅ Replaced |

## Migration Phases

### Phase 1: Wire dlt Sources to CLI (estimated: 1 session)

**Goal**: All 8 CLI commands work with dlt sources as primary, fall back to legacy.

**Changes required**:
1. Add `--source` flag to existing CLI commands (default `"auto"` — try dlt first, fallback to legacy)
2. Create thin wrapper functions that dispatch to dlt or legacy based on availability
3. No legacy code removed — CLI gains dlt as an option

**Files to modify**:
- `cli/archive.py` — add source dispatch to each command handler

**Risk**: Low. CLI already has the command structure; adding a dispatch layer is straightforward.

### Phase 2: Consolidate Sink Workflow (estimated: 1 session)

**Goal**: Replace `SinkWorkflow` (621 lines) with dlt + Polars + Pandera transforms.

**Changes required**:
1. Move `_bronze_*_to_silver()` functions from `sink.py` into `transforms/` (already done)
2. Move `_write_ducklake()` and `DuckLakeCatalog` usage into `workflow/db.py` (partially done)
3. Update `sink` CLI command to use new transform pipeline
4. Deprecate `SinkWorkflow` class (keep for backward compat, mark `@deprecated`)

**Files to modify**:
- `workflow/sink.py` — add deprecation warning, delegate to dlt pipeline
- `cli/archive.py` — update `sink_command` to use new pipeline

**Risk**: Low. All transform functions are already extracted. The `DuckLakeCatalog` usage is minimal.

### Phase 3: Consolidate Gap-Fill (estimated: 1 session)

**Goal**: Replace `GapFillWorkflow` (468 lines) with dlt REST sources + DuckDB gap detection.

**Changes required**:
1. Replace `gap-fill` CLI command to use `dlt_sqlmesh_pipeline(data_type=..., source='rest')`
2. Gap detection moves to `gap_detection.py` (already there)
3. Deprecate `GapFillWorkflow` class

**Files to modify**:
- `workflow/gap_fill.py` — deprecate, delegate to dlt pipeline
- `cli/archive.py` — update `gap_fill_command`

**Risk**: Medium. The original gap-fill saves filled CSVs in a specific directory format that external tooling may depend on.

### Phase 4: Consolidate Health Check (estimated: 1 session)

**Goal**: Replace archive-level health checks with DuckLake-based checks.

**Changes required**:
1. `HealthCheckWorkflow` archive scan → use `ArchiveExplorer` to check archive vs DuckLake
2. Keep `check_ducklake_anomalies()` — already shared
3. Add DuckLake freshness SLA check as Prefect flow

**Files to modify**:
- `workflow/health_check.py` — deprecate archive-level checks, keep DuckLake checks
- `workflow/explorer.py` — add freshness check method
- `cli/archive.py` — update `health_command`

**Risk**: Low. DuckLake anomaly detection already shared.

### Phase 5: Consolidate Metadata (estimated: 1 session)

**Goal**: Replace `MetadataWorkflow` (234 lines) with dlt metadata sources.

**Changes required**:
1. `MetadataWorkflow.refresh_venues()` returns hardcoded venues — replace with `venues_resource()` S3 scan
2. `MetadataWorkflow.refresh_symbols()` — replace with `symbols_resource()`
3. `MetadataWorkflow._save_ducklake()` — replace with dlt DuckLake destination

**Files to modify**:
- `workflow/metadata.py` — deprecate, delegate to `binance_metadata.py`
- `cli/archive.py` — update `refresh_metadata_command`

**Risk**: Low. `binance_metadata.py` already does all of this.

### Phase 6: Remove Dead Code (estimated: 0.5 session)

**Goal**: Remove or archive code that has no callers.

**Removals**:
1. `datacontract.py` (443 lines) — zero callers, move to `docs/proposals/` per AGENTS.md accuracy rule
2. `lineage.py` (401 lines) — only used by legacy workflows, move to `workflow/legacy/`
3. `catalog.py` (DuckLakeCatalog 365 lines) — replaced by `dlt.destinations.ducklake()`, move to `workflow/legacy/`

**Risk**: Low. All confirmed unused or replaced.

### Phase 7: CLI Migration (estimated: 2 sessions)

**Goal**: All CLI commands use dlt sources by default.

**Changes required**:
1. Default `--source` to `"dlt"` instead of `"auto"`
2. Remove fallback to legacy workflows after testing
3. Update help text and documentation

**Files to modify**:
- `cli/archive.py` — final switch to dlt defaults
- `docs/` — update CLI reference

**Risk**: Medium. CLI is the primary user interface; changes must be well-tested.

## Test Migration

| Phase | Legacy Tests | New Stack Tests | Action |
|-------|-------------|-----------------|--------|
| 1 | `test_cli.py` (1,026) | Add dlt CLI tests | Extend, don't replace |
| 2-5 | `test_archive_workflow.py` (801) | `test_dlt_sources.py` (229) + `test_transforms.py` (232) + `test_validation.py` (302) | Keep legacy tests until phases complete |
| 6 | — | — | Remove tests for deleted code |

**Principle**: Legacy tests stay until the corresponding legacy code is removed. New stack tests already cover the new functionality.

## Rollback Plan

Each phase is independently reversible:
1. **CLI dispatch**: Remove `--source` flag → CLI uses legacy workflows
2. **Sink**: Restore `SinkWorkflow` import → sink uses legacy
3. **Gap-fill**: Restore `GapFillWorkflow` import → gap-fill uses legacy
4. **Health**: Archive-level checks are additive — just stop calling them
5. **Metadata**: Restore `MetadataWorkflow` import → refresh uses legacy
6. **Dead code removal**: Files moved to `workflow/legacy/` not deleted

## Risk Register

| Risk | Phase | Mitigation |
|------|-------|------------|
| CLI dispatch breaks existing users | 1 | Default `--source=auto` — detects dlt availability |
| _filled CSV format changes | 3 | Keep CSV output path compatible with legacy |
| DuckLake schema mismatch | 2 | Pandera validation catches drift at pipeline boundaries |
| Test coverage gap | All | Legacy tests kept until legacy code removed; new tests added per phase |
