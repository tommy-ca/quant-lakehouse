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
| Polars transforms | 3 (klines, aggTrades, fundingRate) |
| Pandera schemas | 4 (bronze klines, silver klines, silver aggTrades, silver fundingRate) |
| Pydantic models | 3 (KlineModel, AggTradeModel, FundingRateModel) |
| SQLMesh models | 3 (bronze VIEW, silver INCREMENTAL, 2 audits) |
| Prefect flows | 9 (6 legacy + 2 metadata + 1 dlt) |
| E2E pipeline | Works (dlt→DuckDB roundtrip verified) |

## Audit Findings

### Resolved Issues

1. **`numpy` not in dependencies**: ✅ **FIXED**. Replaced all `fetchdf()` calls with
   DuckDB's native `.pl()` (Polars) output. DuckDB 1.5.2+ returns `pl.DataFrame`
   directly — no pandas/numpy required. 6 call sites across 3 transform tasks updated.

2. **No Polars transforms for aggTrades/fundingRate**: ✅ **FIXED**. Added
   `transforms/agg_trades.py` and `transforms/funding_rate.py` with Pandera schemas
   (`AggTradesSilverSchema`, `FundingRateSilverSchema`) and Prefect tasks.

3. **No Prefect tasks for 3 of 4 dlt sources**: ✅ **FIXED**. Added `run_dlt_archive`,
   `run_dlt_agg_trades`, `run_dlt_funding_rate`, `run_dlt_ws` tasks, plus
   `transform_agg_trades_to_silver` and `transform_funding_rate_to_silver`.

4. **`dlt_sqlmesh_pipeline` only handles klines**: ✅ **FIXED**. Extended with
   `data_type` and `source` parameters dispatching to correct tasks.

### Remaining Issues

1. **Invalid SQLMesh bronze model**: `models/bronze/klines.sql` references the physical
   table `bronze.BTCUSDT_klines` (hardcoded symbol). Multi-symbol ingestion would
   create multiple tables but the VIEW only reads one.

2. **SQLMesh not in dependencies**: `run_sqlmesh_plan` task has a graceful
   `ImportError` fallback, but the SQLMesh project models exist and would fail
   at plan time without sqlmesh installed.

3. **Pandera `isin` validator broken**: Pandera 0.31.x Polars backend has a
   known issue with `isin` on string columns. Removed from schemas.

4. **Prefect flow doesn't expose Pandera validation step**: Validation happens
   transparently inside `transform_to_silver` but isn't visible in flow output.

5. **dlt resource naming convention**: Table names are lowercased (e.g.
   `rest_btcusdt_agg_trades`). Querying requires knowing dlt's normalization rules.

## Proposed Plan

### Phase 1: ✅ Complete

All critical issues resolved in previous sessions:

1. ✅ numpy/pandas → DuckDB `.pl()` for all transforms
2. ✅ `transforms/agg_trades.py` — bronze→silver with Pandera
3. ✅ `transforms/funding_rate.py` — bronze→silver with Pandera
4. ✅ Pandera schemas: `AggTradesSilverSchema`, `FundingRateSilverSchema`
5. ✅ Prefect tasks: `run_dlt_archive`, `run_dlt_agg_trades`, `run_dlt_funding_rate`, `run_dlt_ws`
6. ✅ `dlt_sqlmesh_pipeline` extended with `data_type` + `source` dispatch

### Phase 2: Remaining (1 session)

**Tasks**:
1. Fix SQLMesh bronze model to use dynamic table resolution or parameterized symbol
2. Add `pandera` validation as explicit stage in flow output
3. Research Pandera `isin` fix or maintain workaround

### Phase 3: Future

**Tasks**:
1. Add SQLMesh to dependencies (currently graceful fallback)
2. Add WS streaming as production Prefect deployment with proper backpressure

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
