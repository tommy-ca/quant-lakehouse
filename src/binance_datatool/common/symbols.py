"""Helpers for inferring symbol metadata from Binance symbol strings.

Each ``infer_*`` function accepts a raw symbol as it appears in the
data.binance.vision S3 listing and returns a typed dataclass, or ``None``
when the symbol cannot be parsed.
"""

from __future__ import annotations

import re

from binance_datatool.common.constants import (
    LEVERAGE_EXCLUDES,
    LEVERAGE_SUFFIXES,
    QUOTE_ASSETS,
    QUOTE_BASE_EXCLUDES,
    STABLECOINS,
)
from binance_datatool.common.enums import ContractType, TradeType
from binance_datatool.common.types import CmSymbolInfo, SpotSymbolInfo, UmSymbolInfo

_SETTLED_SUFFIX_RE = re.compile(r"_?SETTLED\d*$")


def _strip_settled_suffix(symbol: str) -> str:
    """Strip the optional settled suffix used for delisted or split symbols."""

    return _SETTLED_SUFFIX_RE.sub("", symbol)


def _should_skip_quote_match(symbol: str, quote: str) -> bool:
    """Return whether a greedy quote match should fall back to a shorter quote."""

    rule = QUOTE_BASE_EXCLUDES.get(quote)
    if rule is None:
        return False

    fallback_quote, bases = rule
    return symbol.endswith(fallback_quote) and symbol[: -len(fallback_quote)] in bases


def infer_spot_info(symbol: str) -> SpotSymbolInfo | None:
    """Infer metadata from a spot symbol string."""
    cleaned = _strip_settled_suffix(symbol)
    for quote in QUOTE_ASSETS:
        if not cleaned.endswith(quote):
            continue

        base = cleaned[: -len(quote)]
        if not base:
            continue
        if _should_skip_quote_match(cleaned, quote):
            continue

        return SpotSymbolInfo(
            symbol=symbol,
            base_asset=base,
            quote_asset=quote,
            is_leverage=base.endswith(LEVERAGE_SUFFIXES) and base not in LEVERAGE_EXCLUDES,
            is_stable_pair=base in STABLECOINS and quote in STABLECOINS,
        )

    return None


def infer_um_info(symbol: str) -> UmSymbolInfo | None:
    """Infer metadata from a USD-M futures symbol string."""
    cleaned = _strip_settled_suffix(symbol)
    if "_" in cleaned:
        contract_type = ContractType.delivery
        cleaned = cleaned.split("_", maxsplit=1)[0]
    else:
        contract_type = ContractType.perpetual

    for quote in QUOTE_ASSETS:
        if not cleaned.endswith(quote):
            continue

        base = cleaned[: -len(quote)]
        if not base:
            continue
        if _should_skip_quote_match(cleaned, quote):
            continue

        return UmSymbolInfo(
            symbol=symbol,
            base_asset=base,
            quote_asset=quote,
            contract_type=contract_type,
            is_stable_pair=base in STABLECOINS and quote in STABLECOINS,
        )

    return None


def infer_cm_info(symbol: str) -> CmSymbolInfo | None:
    """Infer metadata from a COIN-M futures symbol string."""
    cleaned = _strip_settled_suffix(symbol)
    if "_" not in cleaned:
        return None

    underlying, suffix = cleaned.split("_", maxsplit=1)
    if suffix == "PERP":
        contract_type = ContractType.perpetual
    elif suffix.isdigit():
        contract_type = ContractType.delivery
    else:
        return None

    if not underlying.endswith("USD"):
        return None

    base = underlying[:-3]
    if not base:
        return None

    return CmSymbolInfo(
        symbol=symbol,
        base_asset=base,
        quote_asset="USD",
        contract_type=contract_type,
    )


def resolve_symbols(trade_type: str | TradeType, symbols: list[str]) -> list[str]:
    """Resolves requested symbol aliases to trade-type appropriate identifiers."""
    tt = TradeType(trade_type)
    resolved = []
    for sym in symbols:
        if tt == TradeType.cm:
            # Map USDT-pair alias to USD-pair for COIN-M
            if "USDT" in sym:
                resolved.append(sym.replace("USDT", "USD_PERP"))
            else:
                resolved.append(sym)
        else:
            resolved.append(sym)
    return resolved
