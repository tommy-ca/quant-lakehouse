"""Data contract definitions and validation for binance-datatool.

A data contract specifies the schema, structure, and validation rules for a dataset
from a specific source and market type. Contracts enable:

- Schema validation on ingested data
- Type checking (column types, nullability)
- Custom business rule validation (price > 0, volume >= 0, etc.)
- Lineage tracking with contract metadata
- Consistent downstream processing (lakes, feature stores, analytics)

Example:
    ```python
    # Define a contract
    contract = DataContract(
        source=DataSource.BINANCE,
        market_type=MarketType.SPOT,
        data_type=DataType.KLINES,
        partition_freq=PartitionFreq.DAILY,
        schema={
            "open_time": int,
            "open": Decimal,
            "high": Decimal,
            "low": Decimal,
            "close": Decimal,
            "volume": Decimal,
            "close_time": int,
            "quote_volume": Decimal,
            "num_trades": int,
            "taker_buy_volume": Decimal,
            "taker_buy_quote_volume": Decimal,
        },
        key_cols=["open_time"],
        validators=[
            lambda row: row["open"] > 0,
            lambda row: row["close"] > 0,
        ]
    )

    # Validate a dataframe
    result = contract.validate(df)
    if result.passed:
        print(f"✓ {result.row_count} rows valid")
    else:
        print(f"✗ Errors: {result.errors}")
    ```
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


class DataSource(StrEnum):
    BINANCE = "binance"
    BYBIT = "bybit"
    OKX = "okx"


class MarketType(StrEnum):
    """Market segment for a trading pair."""

    SPOT = "spot"
    FUTURES_USD_M = "um"  # USD-M perpetual/delivery
    FUTURES_COIN_M = "cm"  # COIN-M perpetual/delivery
    OPTIONS = "options"


class DataType(StrEnum):
    """Specific dataset type within a market."""

    KLINES = "klines"  # OHLCV candlesticks
    AGGR_TRADES = "aggTrades"  # Aggregated trades
    TRADES = "trades"  # Raw trades
    BOOK_DEPTH = "bookDepth"  # Order book depth
    BOOK_TICKER = "bookTicker"  # Best bid/ask
    FUNDING_RATE = "fundingRate"  # Perpetual funding


class PartitionFreq(StrEnum):
    """Temporal partitioning frequency."""

    DAILY = "daily"
    MONTHLY = "monthly"
    HOURLY = "hourly"


@dataclass(frozen=True)
class ValidationError:
    """A single validation failure."""

    row_index: int | None = None  # None for schema-level errors
    column: str | None = None
    reason: str = ""  # Human-readable message
    value: Any | None = None  # The problematic value


@dataclass
class ValidationResult:
    """Result of validating data against a contract."""

    passed: bool
    row_count: int = 0
    error_count: int = 0
    errors: list[ValidationError] = field(default_factory=list)
    duration_seconds: float = 0.0

    def __repr__(self) -> str:
        status = "✓" if self.passed else "✗"
        return (
            f"{status} ValidationResult(rows={self.row_count}, "
            f"errors={self.error_count}, duration={self.duration_seconds:.2f}s)"
        )


@dataclass
class DataContract:
    """Formal contract (schema + rules) for a dataset.

    Attributes:
        source: Data source (exchange, provider).
        market_type: Market segment (spot, futures, etc.).
        data_type: Dataset type (klines, trades, etc.).
        partition_freq: Temporal partitioning strategy.
        schema: Column name → expected type (dict).
        partition_cols: Column names used for partitioning.
        key_cols: Column names that form the primary key.
        validators: List of validation functions (row → bool).
        nullable_cols: Column names that may contain NULL values.
        description: Human-readable description.
    """

    source: DataSource
    market_type: MarketType
    data_type: DataType
    partition_freq: PartitionFreq

    schema: dict[str, type] = field(default_factory=dict)
    partition_cols: list[str] = field(default_factory=list)
    key_cols: list[str] = field(default_factory=list)
    nullable_cols: set[str] = field(default_factory=set)
    validators: list[Callable[[dict[str, Any]], bool]] = field(default_factory=list)
    description: str = ""

    def __post_init__(self) -> None:
        """Validate contract configuration."""
        if not self.schema:
            raise ValueError("DataContract.schema must not be empty")
        if not self.key_cols:
            raise ValueError("DataContract.key_cols must not be empty")

        # Ensure key_cols are in schema
        for col in self.key_cols:
            if col not in self.schema:
                raise ValueError(f"Key column '{col}' not found in schema")

        # Note: partition_cols may be external to schema (inferred from paths)
        # so we don't validate them against schema

    def validate(self, data: list[dict[str, Any]] | Any) -> ValidationResult:
        """Validate data against this contract.

        Supports list of dicts or dataframe-like objects (with .to_dict(),
        .shape, .dtypes attributes).

        Args:
            data: List of rows (dicts) or dataframe-like object.

        Returns:
            ValidationResult with passed status, error count, and details.
        """
        import time

        start_time = time.time()
        rows = self._normalize_input(data)
        row_count = len(rows)

        errors: list[ValidationError] = []

        # Schema validation (once per contract)
        if not rows:
            # Empty dataset is valid (may be expected)
            return ValidationResult(
                passed=True,
                row_count=0,
                error_count=0,
                errors=[],
                duration_seconds=time.time() - start_time,
            )

        # Check schema on first row
        first_row = rows[0]
        for col, expected_type in self.schema.items():  # noqa: B007
            if col not in first_row:
                errors.append(
                    ValidationError(
                        row_index=None,
                        column=col,
                        reason="Missing column in schema",
                    )
                )

        # Per-row validation
        for idx, row in enumerate(rows):
            # Type checking
            for col, expected_type in self.schema.items():
                if col not in row:
                    if col not in self.nullable_cols:
                        errors.append(
                            ValidationError(
                                row_index=idx,
                                column=col,
                                reason="Missing column",
                            )
                        )
                    continue

                value = row[col]

                # Check NULL/None
                if value is None:
                    if col not in self.nullable_cols:
                        errors.append(
                            ValidationError(
                                row_index=idx,
                                column=col,
                                reason="NULL value not allowed",
                                value=value,
                            )
                        )
                    continue

                # Check type
                if not isinstance(value, expected_type):
                    errors.append(
                        ValidationError(
                            row_index=idx,
                            column=col,
                            reason=(
                                f"Type mismatch: "
                                f"expected {expected_type.__name__}, "
                                f"got {type(value).__name__}"
                            ),
                            value=value,
                        )
                    )

            # Run custom validators
            for validator_func in self.validators:
                try:
                    if not validator_func(row):
                        errors.append(
                            ValidationError(
                                row_index=idx,
                                reason=f"Validator failed: {validator_func.__name__}",
                                value=row,
                            )
                        )
                except Exception as exc:
                    errors.append(
                        ValidationError(
                            row_index=idx,
                            reason=f"Validator error: {exc}",
                            value=row,
                        )
                    )

        duration = time.time() - start_time
        return ValidationResult(
            passed=len(errors) == 0,
            row_count=row_count,
            error_count=len(errors),
            errors=errors,
            duration_seconds=duration,
        )

    def _normalize_input(self, data: list[dict[str, Any]] | Any) -> list[dict[str, Any]]:
        """Convert various input formats to list[dict].

        Supports:
        - list[dict]: returned as-is
        - pandas.DataFrame: converted via .to_dict('records')
        - polars.DataFrame: converted via .to_dicts()
        """
        if isinstance(data, list):
            return data

        # Try pandas
        if hasattr(data, "to_dict") and callable(data.to_dict):
            try:
                return data.to_dict("records")
            except Exception:
                pass

        # Try polars
        if hasattr(data, "to_dicts") and callable(data.to_dicts):
            try:
                return data.to_dicts()
            except Exception:
                pass

        raise TypeError(
            f"Cannot normalize data of type {type(data).__name__}; "
            "expected list[dict], pandas.DataFrame, or polars.DataFrame"
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize contract to dict (for storage, config files)."""
        return {
            "source": self.source.value,
            "market_type": self.market_type.value,
            "data_type": self.data_type.value,
            "partition_freq": self.partition_freq.value,
            "schema": {k: v.__name__ for k, v in self.schema.items()},
            "partition_cols": self.partition_cols,
            "key_cols": self.key_cols,
            "nullable_cols": list(self.nullable_cols),
            "description": self.description,
            # Note: validators are not serialized (functions are not JSON-able)
        }


# Standard contracts for common use cases

BINANCE_SPOT_KLINES_CONTRACT = DataContract(
    source=DataSource.BINANCE,
    market_type=MarketType.SPOT,
    data_type=DataType.KLINES,
    partition_freq=PartitionFreq.DAILY,
    schema={
        "open_time": int,
        "open": Decimal,
        "high": Decimal,
        "low": Decimal,
        "close": Decimal,
        "volume": Decimal,
        "close_time": int,
        "quote_volume": Decimal,
        "num_trades": int,
        "taker_buy_volume": Decimal,
        "taker_buy_quote_volume": Decimal,
    },
    partition_cols=["date", "symbol"],
    key_cols=["open_time"],
    validators=[
        lambda row: row["open"] > 0,
        lambda row: row["high"] >= row["open"],
        lambda row: row["high"] >= row["close"],
        lambda row: row["low"] <= row["open"],
        lambda row: row["low"] <= row["close"],
        lambda row: row["low"] <= row["high"],
        lambda row: row["close"] > 0,
        lambda row: row["volume"] >= 0,
        lambda row: row["quote_volume"] >= 0,
        lambda row: row["close_time"] > row["open_time"],
    ],
    description=(
        "Binance spot market daily OHLCV (klines) with validation rules "
        "for price ordering and non-negative volumes"
    ),
)

BINANCE_UM_KLINES_CONTRACT = DataContract(
    source=DataSource.BINANCE,
    market_type=MarketType.FUTURES_USD_M,
    data_type=DataType.KLINES,
    partition_freq=PartitionFreq.DAILY,
    schema={
        "open_time": int,
        "open": Decimal,
        "high": Decimal,
        "low": Decimal,
        "close": Decimal,
        "volume": Decimal,
        "close_time": int,
        "quote_volume": Decimal,
        "num_trades": int,
        "taker_buy_volume": Decimal,
        "taker_buy_quote_volume": Decimal,
    },
    partition_cols=["date", "symbol"],
    key_cols=["open_time"],
    validators=[
        lambda row: row["open"] > 0,
        lambda row: row["high"] >= row["low"],
        lambda row: row["close"] > 0,
        lambda row: row["volume"] >= 0,
    ],
    description=(
        "Binance USD-M futures daily OHLCV (klines) with basic price and volume validation"
    ),
)


class ContractRegistry:
    """Registry for looking up data contracts by (source, market, data_type)."""

    _contracts: dict[tuple[DataSource, MarketType, DataType], DataContract] = {
        (
            DataSource.BINANCE,
            MarketType.SPOT,
            DataType.KLINES,
        ): BINANCE_SPOT_KLINES_CONTRACT,
        (
            DataSource.BINANCE,
            MarketType.FUTURES_USD_M,
            DataType.KLINES,
        ): BINANCE_UM_KLINES_CONTRACT,
    }

    @classmethod
    def register(
        cls,
        source: DataSource,
        market_type: MarketType,
        data_type: DataType,
        contract: DataContract,
    ) -> None:
        """Register a contract for a (source, market, data_type) tuple."""
        key = (source, market_type, data_type)
        cls._contracts[key] = contract

    @classmethod
    def get(
        cls,
        source: DataSource,
        market_type: MarketType,
        data_type: DataType,
    ) -> DataContract | None:
        """Retrieve a registered contract."""
        key = (source, market_type, data_type)
        return cls._contracts.get(key)

    @classmethod
    def all(cls) -> dict[tuple[DataSource, MarketType, DataType], DataContract]:
        """Return all registered contracts."""
        return cls._contracts.copy()
