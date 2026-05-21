# Brainstorm & Plan: Point-in-Time Universe Construction

## 1. Context & Motivation
Currently, the `UniverseBuilder` and `RateProvider` use the "latest" state from the `registry.instruments` and `registry.market_stats` tables, and `UniverseBuilder` calculates asset age based on `time.time()`.
If a quantitative researcher wants to backtest a strategy over the year 2024, they need the top 50 universe *as it existed on January 1, 2024*. Using the current 2026 top 50 would introduce massive **survivorship bias** and **look-ahead bias**, as it would include tokens that succeeded and exclude those that failed or were delisted since 2024.

## 2. Deep Audit of Current State
- `UniverseBuilder.build_top_50`:
  - `now_ms = int(time.time() * 1000)` is hardcoded.
  - The query uses `i.status = 'trading'`. If we look back in time, we might need a history of instrument statuses, or we assume `onboard_date` is the primary factor. Wait, `registry.market_stats` has `last_price` and `market_cap` which are current. `registry.instruments` has `status` which is current.
  - To truly do point-in-time, we would need historical volume and market cap data. However, the current registry table only stores the *latest* snapshot.

## 3. The Challenge of "Point-in-Time" with Current Data Model
- The `registry.instruments` and `registry.market_stats` are snapshot tables. They do not contain temporal history of volume, market cap, or status (delistings).
- If we pass `as_of_timestamp_ms` to `build_top_50`, we can definitely filter out assets where `onboard_date > as_of_timestamp_ms` (i.e., assets that didn't exist yet).
- **BUT**, we cannot know what the volume or market cap was on Jan 1, 2024, using `registry.market_stats`. We also don't know if a currently delisted token *was* trading on Jan 1, 2024, unless we have a history.
- To fully solve survivorship bias, the catalog needs SCD Type 2 (Slowly Changing Dimensions) or we need to compute point-in-time metrics from the Bronze/Silver klines directly.
- **MVP Approach**:
  1. Add `as_of_timestamp_ms` to `build_top_50`.
  2. Filter out instruments where `onboard_date > as_of_timestamp_ms`.
  3. Calculate age relative to `as_of_timestamp_ms` rather than `time.time()`.
  4. Acknowledge in the documentation that volume/market cap are based on the latest snapshot, meaning true point-in-time backtesting requires joining against historical silver klines (which could be a future feature).

## 4. Implementation Plan
1. **Update `UniverseBuilder` Signature**: Add `as_of_timestamp_ms: int | None = None` to `build_top_50`.
2. **Age Calculation**: Use `now_ms = as_of_timestamp_ms if as_of_timestamp_ms is not None else int(time.time() * 1000)`.
3. **Future-Existence Filter**: Add `pl.col("onboard_date") <= now_ms`.
4. **TDD**: Update `tests/test_universe_builder.py` to assert that tokens listed *after* the `as_of_timestamp_ms` are excluded.
5. **Documentation**: Update `UNIVERSE_SPEC.md` and `DATA_FLOWS.md` to describe the point-in-time `as_of_timestamp_ms` capability and its current snapshot-data limitations regarding volume/market cap.
