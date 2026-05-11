"""Pydantic models for dlt resource validation.

Each model maps to a dlt resource and enforces per-record validation
(via ``@field_validator``) so that bad rows are caught before normalization.

Used with dlt's Pydantic integration (``columns=KlineModel`` in
``@dlt.resource``) for authoritative schema + validation.
"""

from __future__ import annotations

from typing import ClassVar

from dlt.common.libs.pydantic import DltConfig  # noqa: TC002 — used at runtime for ClassVar
from pydantic import BaseModel, field_validator


class KlineModel(BaseModel):
    """Pydantic model for a single kline record in dlt pipelines.

    Used as the authoritative schema for dlt resources::

        @dlt.resource(name="klines", columns=KlineModel)
        def klines_resource(...): ...

    dlt infers column types from Pydantic field types and applies
    ``@field_validator`` constraints before normalization.
    """

    dlt_config: ClassVar[DltConfig] = {
        "is_authoritative_model": True,
    }

    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: int
    quote_volume: float | None = None
    count: int = 0
    taker_buy_volume: float | None = None
    taker_buy_quote_volume: float | None = None
    symbol: str
    interval: str

    @field_validator("high")
    @classmethod
    def high_ge_low(cls, v: float, info) -> float:
        if "low" in info.data and v < info.data["low"]:
            raise ValueError(f"high ({v}) < low ({info.data['low']})")
        return v

    @field_validator("close")
    @classmethod
    def close_ge_low(cls, v: float, info) -> float:
        if "low" in info.data and v < info.data["low"]:
            raise ValueError(f"close ({v}) < low ({info.data['low']})")
        return v

    @field_validator("volume")
    @classmethod
    def volume_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"volume ({v}) < 0")
        return v

    @field_validator("open_time")
    @classmethod
    def open_time_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError(f"open_time ({v}) <= 0")
        return v


class AggTradeModel(BaseModel):
    """Pydantic model for a single aggTrade record in dlt pipelines."""

    dlt_config: ClassVar[DltConfig] = {
        "is_authoritative_model": True,
    }

    agg_trade_id: int
    price: float
    quantity: float
    transact_time: int
    is_buyer_maker: bool
    symbol: str

    @field_validator("price")
    @classmethod
    def price_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(f"price ({v}) <= 0")
        return v

    @field_validator("quantity")
    @classmethod
    def quantity_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"quantity ({v}) < 0")
        return v


class FundingRateModel(BaseModel):
    """Pydantic model for a single fundingRate record in dlt pipelines."""

    dlt_config: ClassVar[DltConfig] = {
        "is_authoritative_model": True,
    }

    symbol: str
    funding_time: int
    funding_rate: float
