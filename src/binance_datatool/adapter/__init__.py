"""Adapter helpers and protocol for multi-source support.

This package provides a small DataSourceAdapter protocol and a lightweight
BinanceAdapter that wraps the existing ArchiveClient. The goal is to keep
the adapter surface minimal (KISS) while allowing workflows to depend on a
stable abstraction (SOLID: Dependency Inversion). See docs for usage.
"""

from __future__ import annotations

from .binance import BinanceAdapter
from .protocol import DataSourceAdapter

__all__ = ["DataSourceAdapter", "BinanceAdapter"]
