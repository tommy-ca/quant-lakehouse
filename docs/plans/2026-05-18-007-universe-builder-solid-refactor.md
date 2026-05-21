# Plan: UniverseBuilder SOLID Refactor

## 1. Context
Following the implementation of the institutional exclusion filters (memes, leveraged tokens, non-ASCII symbols) and the corresponding TDD tests, an audit reveals that `UniverseBuilder` still contains a SOLID principle violation. Specifically, the `build_top_50` method hardcodes a dictionary of static fallback rates for USD normalization:

```python
            rates = {
                "USDT": 1.0, "USDC": 1.0, "BUSD": 1.0, "DAI": 1.0, "FDUSD": 1.0, "TUSD": 1.0, "USD": 1.0,
                "TRY": 0.03, "IDR": 0.00006, "JPY": 0.006, # Statics as fallback
            }
```
This violates the Open/Closed Principle and makes the component harder to test or configure for different environments.

## 2. Proposed Changes
To fully align with the previous brainstorms (`2026-05-18-top50-universe-audit-and-plan.md`):

1. **Extract `RateProvider`**: Create a new class `RateProvider` in `src/binance_datatool/universe/rates.py`. This class will be responsible for returning fallback rates or fetching dynamic rates from the DuckDB catalog.
2. **Inject Dependency**: Inject the `RateProvider` into `UniverseBuilder` so that it handles rate resolution cleanly, allowing mock providers in testing.
3. **Specs & Documentation Update**: Update `docs/UNIVERSE_SPEC.md` and `docs/DATA_FLOWS.md` to document the new `RateProvider` component.

## 3. Execution (TDD Flow)
- **Step 1**: Write tests for `RateProvider` in `tests/test_rate_provider.py`.
- **Step 2**: Update `tests/test_universe_builder.py` to use a mocked `RateProvider`.
- **Step 3**: Implement `RateProvider` in `src/binance_datatool/universe/rates.py`.
- **Step 4**: Refactor `UniverseBuilder` in `src/binance_datatool/universe/builder.py` to use `RateProvider`.
- **Step 5**: Run pytest to verify all tests pass.
- **Step 6**: Update `docs/UNIVERSE_SPEC.md` and `docs/DATA_FLOWS.md`.
