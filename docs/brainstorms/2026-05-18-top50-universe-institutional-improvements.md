# Brainstorm & Plan: Institutional Universe Improvements

## 1. Context & Issues Spotted
A review of the recently generated top 50 spot and USD-M futures universes revealed the inclusion of assets that do not meet institutional standards:
- **Meme / Political Tokens:** `TRUMPUSDT`, `FARTCOINUSDT`, `PEPEUSDT`, `SHIBUSDT`, `DOGEUSDT` (some institutions allow DOGE/SHIB, but often strictly filter out lower-tier memes).
- **Non-Standard / Junk Assets:** `币安人生USDT` (contains non-ASCII characters).
- **Leveraged Tokens:** While maybe not in the immediate top 50, Binance spot has assets like `BTCUP`, `BTCDOWN`.
- **Multiplier Contracts:** USD-M futures contain `1000PEPEUSDT`, `1000SHIBUSDT`, `1000LUNCUSDT`. These are valid contracts for those assets but can distort analysis if not normalized or explicitly handled.

According to institutional indices (like Bitwise 10 and S&P Cryptocurrency Series), an investable universe requires:
1. **Stringent Risk Screening:** Removing assets with regulatory, technical, or qualitative risks (e.g., meme coins, non-compliant assets).
2. **Quality Control:** Enforcing name constraints and exchange presence.

## 2. Proposed Improvements (TDD & Specs Driven)
To align the `UniverseBuilder` with institutional standards (SOLID, KISS, YAGNI), we will implement the following changes:

### A. New Exclusion Filters
1. **Leveraged Token Filter:** Utilize the existing `LEVERAGE_SUFFIXES` and `LEVERAGE_EXCLUDES` from `src/binance_datatool/common/constants.py` to filter out leveraged spot tokens (e.g., UP/DOWN/BULL/BEAR).
2. **Non-ASCII Filter:** Enforce a strict ASCII-only rule for asset symbols to avoid junk or scam tokens (e.g., `币安人生USDT`).
3. **Meme / High-Risk Filter (Optional Toggle):** Introduce a flag `exclude_memes=True` (defaulting to True for institutional focus) that filters out known high-risk/meme tokens defined in a new constant `MEME_COINS`.

### B. Implementation Plan
1. **Constants Update:**
   - Define `MEME_COINS` in `constants.py`.
2. **Specs Update:**
   - Update `docs/UNIVERSE_SPEC.md` to include these new "Institutional Risk Screens" in the Quality & Stability Filters table.
3. **TDD:**
   - Add tests in `tests/test_universe_builder.py` to assert that leveraged tokens, non-ASCII tokens, and meme tokens are excluded when the respective flags are enabled.
4. **Code Refactoring:**
   - Implement the filters using Polars expressions in `src/binance_datatool/universe/builder.py`.
     - `pl.col("symbol").str.contains(r"^[A-Za-z0-9\-_]+$")` for ASCII validation.
     - Suffix matching for leveraged tokens using `LEVERAGE_SUFFIXES` and `LEVERAGE_EXCLUDES`.
     - `is_in` for `MEME_COINS` (with handling for `1000` prefixes in futures).
5. **Validation:**
   - Run the test suite (`uv run pytest tests/test_universe_builder.py`).
   - Regenerate the top 50 list and verify the absence of flagged assets.
