"""Data validation: Pandera schemas + Pydantic models for pipeline contracts.

Provides runtime validation at pipeline boundaries:
- Pandera ``DataFrameModel`` schemas for Polars DataFrame validation
  (Bronze→Silver transforms, sink writes)
- Pydantic ``BaseModel`` models for dlt resource validation
  (REST API responses, individual kline records)

Usage::

    from binance_datatool.validation.schemas import SilverKlinesSchema

    # Validate a Polars DataFrame before DuckDB insert
    SilverKlinesSchema.validate(df, lazy=True)
"""

from binance_datatool.validation.schemas import BronzeKlinesSchema, SilverKlinesSchema

__all__ = [
    "BronzeKlinesSchema",
    "SilverKlinesSchema",
]
