"""dlt resources for Binance metadata — venues and symbols.

Two paths:
1. Archive: Scans S3 directory structure (discovery + lifecycle range).
2. REST API: Fetches high-fidelity trading constraints (tick size, lot size, etc.).
"""

from __future__ import annotations

import asyncio
import re
from datetime import UTC, datetime
from typing import Any

import dlt
from loguru import logger

from binance_datatool.archive.client import ArchiveClient
from binance_datatool.common.async_utils import sync_run
from binance_datatool.common.enums import DataFrequency, DataType, TradeType
from binance_datatool.dlt.models import InstrumentModel, VenueModel
from binance_datatool.exchange import (
    BinanceCmRestClient,
    BinanceSpotRestClient,
    BinanceUmRestClient,
)
from binance_datatool.workflow.list_symbols import ArchiveListSymbolsWorkflow

ALL_TRADE_TYPES = [TradeType.spot, TradeType.um, TradeType.cm]
DATE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})")


@dlt.resource(
    name="venues",
    write_disposition="merge",
    primary_key="venue_id",
    columns=VenueModel,
)
def venues_resource() -> list[dict[str, Any]]:
    """Load venue metadata aligned with DBN/Tardis schemas."""
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    return [
        {
            "venue_id": "binance_spot",
            "name": "Binance Spot",
            "publisher_id": "BINA.SPOT",
            "dataset": "BINA.SPOT",
            "exchange_slug": "binance",
            "market_type": "spot",
            "base_url": "https://api.binance.com",
            "status": "online",
            "fetched_at": now_ms,
        },
        {
            "venue_id": "binance_um",
            "name": "Binance USD-M Futures",
            "publisher_id": "BINA.UM",
            "dataset": "BINA.UM",
            "exchange_slug": "binance-futures",
            "market_type": "um",
            "base_url": "https://fapi.binance.com",
            "status": "online",
            "fetched_at": now_ms,
        },
        {
            "venue_id": "binance_cm",
            "name": "Binance COIN-M Futures",
            "publisher_id": "BINA.CM",
            "dataset": "BINA.CM",
            "exchange_slug": "binance-delivery",
            "market_type": "cm",
            "base_url": "https://dapi.binance.com",
            "status": "online",
            "fetched_at": now_ms,
        },
    ]


def _parse_filters(filters: list[Any]) -> dict[str, float]:
    """Parse Binance exchangeInfo filters from SDK models."""
    out = {}
    for f_model in filters:
        f = getattr(f_model, "actual_instance", f_model)
        if isinstance(f, dict):
            ft = f.get("filterType")
            if ft == "PRICE_FILTER":
                out["tick_size"] = float(f["tickSize"])
            elif ft == "LOT_SIZE":
                out["lot_size"] = float(f["stepSize"])
            elif ft in ("MIN_NOTIONAL", "NOTIONAL"):
                out["min_notional"] = float(f.get("minNotional") or f.get("notional") or 0.0)
        else:
            ft = getattr(f, "filter_type", getattr(f, "filterType", None))
            if ft == "PRICE_FILTER":
                out["tick_size"] = float(getattr(f, "tick_size", 0.0))
            elif ft == "LOT_SIZE":
                out["lot_size"] = float(getattr(f, "step_size", 0.0))
            elif ft in ("MIN_NOTIONAL", "NOTIONAL"):
                out["min_notional"] = float(getattr(f, "min_notional", getattr(f, "notional", 0.0)))
    return out


@dlt.resource(
    name="instruments",
    write_disposition="merge",
    primary_key=["symbol", "venue_id"],
    columns=InstrumentModel,
)
def instruments_resource(
    trade_types: list[TradeType] | None = None,
    scan_lifecycle: bool = False,
) -> Any:
    """Fetch high-fidelity instrument metadata from Archive and REST."""
    if trade_types is None:
        trade_types = ALL_TRADE_TYPES

    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    archive_client = ArchiveClient()
    rest_clients = {
        TradeType.spot: BinanceSpotRestClient(),
        TradeType.um: BinanceUmRestClient(),
        TradeType.cm: BinanceCmRestClient(),
    }

    for tt in trade_types:
        venue_id = f"binance_{tt.value}"
        logger.info(f"Syncing metadata for {venue_id}...")

        # 1. Discover all symbols from Archive
        wf = ArchiveListSymbolsWorkflow(
            client=archive_client,
            trade_type=tt,
            data_freq=DataFrequency.daily,
            data_type=DataType.klines,
        )
        archive_res = sync_run(wf.run())

        discovered: dict[str, dict] = {}
        for entry in archive_res.matched:
            discovered[entry.symbol] = {
                "symbol": entry.symbol,
                "venue_id": venue_id,
                "base_asset": entry.base_asset,
                "quote_asset": entry.quote_asset,
                "contract_type": tt.value,
                "instrument_class": "spot" if tt == TradeType.spot else "future",
                "status": "delisted",
                "fetched_at": now_ms,
                "is_leverage": getattr(entry, "is_leverage", False),
                "is_stable_pair": getattr(entry, "is_stable_pair", False),
            }

        # 2. Enrich with REST exchangeInfo
        rest_client = rest_clients[tt]
        if tt == TradeType.spot:
            resp = rest_client._client.rest_api.exchange_info()
        else:
            resp = rest_client._client.rest_api.exchange_information()

        data = resp.data()
        for sym in data.symbols:
            s = str(getattr(sym, "symbol", sym.get("symbol") if isinstance(sym, dict) else None))
            if not s:
                continue

            if hasattr(sym, "base_asset"):
                status = str(
                    getattr(sym, "status", getattr(sym, "contract_status", "trading"))
                ).lower()
                p_prec = getattr(sym, "price_precision", getattr(sym, "quote_precision", 0))
                q_prec = getattr(sym, "quantity_precision", getattr(sym, "base_asset_precision", 0))
                filters = sym.filters
                onboard = getattr(sym, "onboard_date", None)
                delivery = getattr(sym, "delivery_date", None)
                base = sym.base_asset
                quote = sym.quote_asset
            else:
                status = str(sym.get("status", sym.get("contractStatus", "trading"))).lower()
                p_prec = sym.get("pricePrecision", sym.get("quotePrecision", 0))
                q_prec = sym.get("quantityPrecision", sym.get("baseAssetPrecision", 0))
                filters = sym.get("filters", [])
                onboard = sym.get("onboardDate")
                delivery = sym.get("deliveryDate")
                base = sym.get("baseAsset")
                quote = sym.get("quoteAsset")

            f_data = _parse_filters(filters)

            meta = discovered.get(
                s,
                {
                    "symbol": s,
                    "venue_id": venue_id,
                    "base_asset": base,
                    "quote_asset": quote,
                    "contract_type": tt.value,
                    "instrument_class": "spot" if tt == TradeType.spot else "future",
                    "fetched_at": now_ms,
                },
            )

            meta.update(
                {
                    "status": status,
                    "tick_size": f_data.get("tick_size", 0.0),
                    "lot_size": f_data.get("lot_size", 0.0),
                    "min_notional": f_data.get("min_notional"),
                    "price_precision": int(p_prec),
                    "qty_precision": int(q_prec),
                    "onboard_date": onboard,
                    "delivery_date": delivery,
                }
            )
            discovered[s] = meta

        # 3. Optional: Scan Lifecycle Range from S3 (Slow)
        if scan_lifecycle:
            logger.info(f"Scanning lifecycle ranges for {len(discovered)} symbols...")

            async def _enrich_ranges(discovered_map: dict, trade_type: TradeType):
                async with archive_client._create_session() as session:
                    tasks = []
                    for s in discovered_map:
                        tasks.append(
                            archive_client.fetch_lifecycle_range(
                                trade_type,
                                DataFrequency.daily,
                                DataType.klines,
                                s,
                                "1m" if trade_type == TradeType.spot else None,
                                session=session,
                            )
                        )
                    ranges = await asyncio.gather(*tasks)
                    for s, (first, last) in zip(discovered_map.keys(), ranges, strict=False):
                        if first:
                            discovered_map[s]["first_data_at"] = int(first.timestamp() * 1000)
                        if last:
                            discovered_map[s]["last_data_at"] = int(last.timestamp() * 1000)

            sync_run(_enrich_ranges(discovered, tt))

        for meta in discovered.values():
            yield meta


@dlt.source
def build_metadata_source(
    trade_types: list[TradeType] | None = None,
    scan_lifecycle: bool = False,
) -> list[dlt.Resource]:
    """Build a dlt source for high-fidelity Binance metadata."""
    return [
        venues_resource(),
        instruments_resource(trade_types=trade_types, scan_lifecycle=scan_lifecycle),
    ]
