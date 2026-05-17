"""Small registry for DataSourceAdapter implementations.

The registry is intentionally tiny: it provides a discoverable mapping from a
string source name to an adapter instance. This keeps the code simple (KISS)
while allowing workflows and CLI code to request adapters by name (Dependency
Inversion). New adapters can register themselves at import time or via
registry.register("name", factory).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from .protocol import DataSourceAdapter


class SourceRegistry:
    """Registry for source adapters.

    Usage:
        registry = SourceRegistry()
        adapter = registry.get("binance")
    """

    def __init__(self) -> None:
        self._factories: dict[str, Callable[[], DataSourceAdapter]] = {}

        # Register built-in adapters lazily to avoid import cycles.
        self.register("binance", self._default_binance_factory)

    def register(self, name: str, factory: Callable[[], DataSourceAdapter]) -> None:
        """Register a factory for an adapter by name."""
        self._factories[name] = factory

    def get(self, name: str) -> DataSourceAdapter | None:
        """Return an adapter instance for the given name, or None if unknown."""
        factory = self._factories.get(name)
        if factory is None:
            return None
        return factory()

    def names(self) -> list[str]:
        return list(self._factories.keys())

    @staticmethod
    def _default_binance_factory() -> DataSourceAdapter:
        # Import locally to keep module import lightweight.
        from binance_datatool.adapter.binance import BinanceAdapter

        return BinanceAdapter()


# Module-level registry singleton for convenience. Import this from other modules
# to discover available adapters without creating multiple registries.
registry = SourceRegistry()
