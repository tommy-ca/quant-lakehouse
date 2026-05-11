---
date: 2026-05-11
topic: audit-findings
status: active
---

# Audit Findings & Implementation Plan

## Baseline (as of 2026-05-11)

| Metric | Value |
|--------|-------|
| Tests passing | 325 (+45 from Phase 0 baseline of 280) |
| Lint | Clean |
| Format | Clean |
| dlt sources | 4 modules (binance, archive, rest, ws) |
| Polars transforms | 1 (klines bronze→silver) |
| Pandera schemas | 2 (bronze klines, silver klines) |
| Pydantic models | 3 (KlineModel, AggTradeModel, FundingRateModel) |
| SQLMesh models | 3 (bronze VIEW, silver INCREMENTAL, 2 audits) |
| Prefect flows | 9 (6 legacy + 2 metadata + 1 dlt) |
| E2E pipeline | Works (dlt→DuckDB roundtrip verified) |

## Audit Findings

### Critical Issues (blocking production use)

1. **`numpy` not in dependencies**: `DuckDBPyConnection.fetchdf()` returns a pandas
   DataFrame, which requires numpy. Any Prefect task using `fetchdf()` (e.g.
   `transform_to_silver`) will crash at runtime if numpy isn't installed. numpy is
   pulled transitively by pandas/dlt but not pinned.

2. **No Prefect tasks for 3 of 4 dlt sources**: Only `run_dlt_source` (REST klines)
   has a Prefect task. The archive source, REST aggTrades/fundingRate, and WS source
   have no Prefect wrappers, so they can't be orchestrated.

3. **No Polars transforms for aggTrades/fundingRate**: The `transforms/` module only
   has `klines.py`. AggTrades and fundingRate data from `binance_rest.py` can't be
   transformed to Silver without their own transform modules.

### Medium Issues

4. **Invalid SQLMesh bronze model**: `models/bronze/klines.sql` references the physical
   table `bronze.BTCUSDT_klines` (hardcoded symbol). Multi-symbol ingestion would
   create multiple tables but the VIEW only reads one.

5. **Pandera schema gaps**: Only klines has Bronze/Silver schemas. aggTrades and
   fundingRate Silver tables have no Pandera validation.

6. **`dlt_sqlmesh_pipeline` only handles klines**: The flow's stages (dlt → Polars
   → SQLMesh) only work for klines data. Other data types not supported.

7. **SQLMesh not in dependencies**: `run_sqlmesh_plan` task has a graceful
   `ImportError` fallback, but the SQLMesh project models exist and would fail
   at plan time without sqlmesh installed.

8. **Prefect flow doesn't expose Pandera validation step**: Validation happens
   inside `transform_to_silver` transparently but isn't visible as a separate
   stage in the flow output.

### Low Issues

9. **dlt resource naming convention**: Table names are lowercased (e.g.
   `rest_btcusdt_agg_trades`). Querying requires knowing dlt's normalization rules.

10. **`transform_to_silver` uses `fetchdf()` (pandas)**: DuckDB Polars integration
    could avoid the pandas dependency entirely via `pl.read_database()`.

11. **Pandera `isin` validator broken**: Pandera 0.31.x Polars backend has a
    known issue with `isin` on string columns. Removed from schemas.

## Proposed Plan

### Phase 1: Fix Critical Issues (1 session)

**Tasks**:
1. Add `numpy` to pyproject.toml dependencies
2. Create `transforms/agg_trades.py` — Polars bronze→silver for aggTrades
3. Create `transforms/funding_rate.py` — Polars bronze→silver for fundingRate
4. Create Pandera schemas for aggTrades Silver and fundingRate Silver
5. Add Prefect tasks: `run_dlt_archive`, `run_dlt_agg_trades`, `run_dlt_funding_rate`, `run_dlt_ws`
6. Extend `dlt_sqlmesh_pipeline` flow to support all data types via dispatcher

### Phase 2: Medium Issues (1 session)

**Tasks**:
1. Fix SQLMesh bronze model to use dynamic table resolution or parameterized symbol
2. Add Pandera Bronze/Silver schemas for aggTrades and fundingRate
3. Fix `dlt_sqlmesh_pipeline` to accept `data_type` param and dispatch to correct transform
4. Add `pandera` to the `_VALIDATION_STEP` in the Prefect flow as an explicit `print`

### Phase 3: Low Issues (future)

**Tasks**:
1. Add `pl.read_database()` path for Polars-native DuckDB reading (no pandas)
2. Research Pandera `isin` fix or maintain removeal

## Architecture Diagram (Post-Phase 1)

```
Legacy CLI commands (unchanged):
  download, verify, gap-fill, sink, health, refresh-metadata

dlt sources (new, Prefect-orchestrated):
  run_dlt_source (REST klines)     → transform_to_silver (klines)
  run_dlt_archive (ZIP klines)     → transform_to_silver (klines)
  run_dlt_agg_trades (REST)        → transform_agg_trades_to_silver
  run_dlt_funding_rate (REST)      → transform_funding_rate_to_silver
  run_dlt_ws (WS streaming)         → transform_to_silver (klines)

                           All → Pandera validate → DuckDB silver

Prefect dlt_sqlmesh_pipeline(data_type):
  switch data_type:
    "klines"       → run_dlt_source → transform_to_silver → SQLMesh
    "aggTrades"    → run_dlt_agg_trades → transform_agg_trades → SQLMesh
    "fundingRate"  → run_dlt_funding_rate → transform_funding_rate → SQLMesh
    "archive"      → run_dlt_archive → transform_to_silver → SQLMesh
    "ws"           → run_dlt_ws → transform_to_silver → SQLMesh
```

## Risk Register

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| aggTrades/fundingRate SDK API differs from klines | Medium | Existing exchange clients are tested (18 exchange tests) |
| WS streaming blocks Prefect task | High | `max_items` parameter limits stream duration; task timeout guard |
| SQLMesh path mismatch | Medium | Config.yaml path resolved relative to project root; dlt pipeline accepts absolute path |
| Pandera Polars backend compatibility | Low | Schema checks limited to column types/nullability; cross-column checks via standalone functions |
