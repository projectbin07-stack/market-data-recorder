"""Source adapters for public, unauthenticated market-data streams."""

from __future__ import annotations

from market_data_recorder.adapters.base import AdapterError, HttpGet, SourceAdapter
from market_data_recorder.adapters.binance import BinanceAdapter
from market_data_recorder.adapters.coinbase import CoinbaseAdapter
from market_data_recorder.adapters.kraken import KrakenAdapter

ADAPTERS: dict[str, type[SourceAdapter]] = {
    BinanceAdapter.name: BinanceAdapter,
    CoinbaseAdapter.name: CoinbaseAdapter,
    KrakenAdapter.name: KrakenAdapter,
}


def get_adapter(name: str) -> SourceAdapter:
    """Instantiate the adapter registered under ``name``."""
    try:
        return ADAPTERS[name.lower()]()
    except KeyError:
        known = ", ".join(sorted(ADAPTERS))
        raise ValueError(f"unknown source {name!r}; choose one of: {known}") from None


__all__ = [
    "ADAPTERS",
    "AdapterError",
    "BinanceAdapter",
    "CoinbaseAdapter",
    "HttpGet",
    "KrakenAdapter",
    "SourceAdapter",
    "get_adapter",
]
