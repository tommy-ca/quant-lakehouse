# Audit & Plan: Top 50 Tradable Universe Construction

## 1. Current State Audit
The `binance-datatool` has a `UniverseBuilder` that consumes metadata from the Lakehouse registry to construct a top-50 tradable asset universe based on liquidity, age, and uniqueness.

### Findings & Issues Spotted
1. **Documentation Drift**:
   - `docs/DATA_FLOWS.md` and `docs/requirements.md` refer to `metadata.duckdb` and `registry.symbols`.
   - The codebase (`metadata_registry.py` and `builder.py`) uses `catalog.duckdb` and `registry.instruments`.
   - **Action**: Align documentation to match the implemented codebase (`catalog.duckdb` and `registry.instruments`).
2. **Missing Test Coverage (TDD Violation)**:
   - There are no tests for `src/binance_datatool/universe/builder.py`.
   - **Action**: Implement `tests/test_universe_builder.py` using pytest fixtures to mock the duckdb connection.
3. **Fragile USD Normalization (SOLID Violation)**:
   - `UniverseBuilder` queries `registry.market_stats` for `BTCUSDT`, `ETHUSDT`, `BNBUSDT` to normalize volume.
   - It includes a hardcoded dictionary of static rates (`TRY`, `JPY`, etc.).
   - This violates the Open/Closed Principle and makes the class hard to test.
   - **Action**: Extract rate fetching to a dedicated service or module (`RateProvider`), or inject it, allowing better mocking and dynamic updates.
4. **Hardcoded Stables List**:
   - The list of stablecoins and fiat is hardcoded in the `build_top_50` method.
   - **Action**: Move this to `constants.py` or configuration.
5. **No Data Flows document specifically highlighting Universe**:
   - `UNIVERSE_SPEC.md` outlines the flow but `DATA_FLOWS.md` needs to reflect this consumer.

## 2. Proposed Improvement Plan (Specs Driven)

### Phase 1: Documentation Alignment
- Update `DATA_FLOWS.md` and `requirements.md` to correctly reference `catalog.duckdb` and `registry.instruments`.
- Incorporate `UniverseBuilder` flow into `DATA_FLOWS.md` as an ELT downstream consumer.

### Phase 2: Refactoring for SOLID & DRY
- Move `stables` to `src/binance_datatool/common/constants.py` as `STABLECOINS_AND_FIAT`.
- Refactor USD rate resolution inside `UniverseBuilder` to gracefully handle missing base rates or use a generalized approach.

### Phase 3: TDD Implementation
- Create `tests/test_universe_builder.py`.
- Mock `get_connection` and return a mock Polars DataFrame representing `registry.instruments` and `registry.market_stats`.
- Test conditions:
  - Empty registry.
  - Exclude delisted items.
  - Correct age filtering (listing > 180 days).
  - Proper scoring and sorting by volume & market cap.

### Phase 4: Implementation Updates
- Update `src/binance_datatool/universe/builder.py` to pass the tests.
