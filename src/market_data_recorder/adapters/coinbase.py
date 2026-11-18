"""Coinbase Exchange public ``matches`` and ``heartbeat`` channels.

Documentation: https://docs.cdp.coinbase.com/exchange/websocket-feed/overview and
https://docs.cdp.coinbase.com/exchange/websocket-feed/channels. Relevant published rules
(see docs/sources.md):

- the unauthenticated endpoint is ``wss://ws-feed.exchange.coinbase.com``;
- a ``subscribe`` message must be sent within 5 seconds of connecting;
- the ``side`` of a match is the *maker* side, so the taker side is the opposite;
- the matches channel can drop messages; the docs recommend comparing the heartbeat's
  ``last_trade_id`` with the last received match and filling holes from REST, which is
  exactly what the gap tracker and backfill do;
- the first message after subscribing is a ``last_match``, used here only as the
  trade-id baseline (it is an old trade, so it is not written as live data);
- client messages are limited to 100 per second per IP; one subscribe covers all symbols.

Backfill uses ``GET /products/{product_id}/trades`` (no authentication) and pages towards
older trades with the ``after`` cursor.
"""

from __future__ import annotations

import json
import time
import urllib.parse
from collections.abc import Callable, Sequence
from typing import Any, ClassVar

from market_data_recorder.adapters.base import (
    AdapterError,
    HttpGet,
    SourceAdapter,
    decimal_field,
    int_field,
    load_json,
    str_field,
)
from market_data_recorder.models import Event, Heartbeat, Notice, TakerSide, Trade
from market_data_recorder.timeutil import iso_to_us

_OPPOSITE: dict[str, TakerSide] = {"buy": "sell", "sell": "buy"}


class CoinbaseAdapter(SourceAdapter):
    name: ClassVar[str] = "coinbase"
    default_url: ClassVar[str] = "wss://ws-feed.exchange.coinbase.com"
    rest_url: ClassVar[str] = "https://api.exchange.coinbase.com"
    ping_interval: ClassVar[float | None] = 20.0
    ping_timeout: ClassVar[float | None] = 20.0
    # Heartbeats arrive every second per product; ten silent seconds means trouble.
    idle_timeout: ClassVar[float | None] = 10.0
    max_symbols: ClassVar[int] = 100
    supports_backfill: ClassVar[bool] = True
    #: Pause between REST pages (public REST limit: 10 requests/s per IP).
    page_pause_s: ClassVar[float] = 0.25

    def subscribe_messages(self, symbols: Sequence[str]) -> list[str]:
        message = {
            "type": "subscribe",
            "product_ids": list(symbols),
            "channels": ["matches", "heartbeat"],
        }
        return [json.dumps(message)]

    def parse(self, raw: str | bytes, recv_us: int) -> list[Event]:
        data = load_json(raw)
        if not isinstance(data, dict):
            raise AdapterError("expected a JSON object")
        kind = data.get("type")
        if kind in ("match", "last_match"):
            trade = self._trade(data, recv_us)
            if kind == "last_match":
                return [Heartbeat(trade.symbol, trade.trade_id)]
            return [trade]
        if kind == "heartbeat":
            return [
                Heartbeat(
                    str_field(data, "product_id"),
                    int_field(data.get("last_trade_id"), "last_trade_id"),
                )
            ]
        if kind == "subscriptions":
            return [Notice("ack", f"subscriptions: {json.dumps(data.get('channels'))}")]
        if kind == "error":
            return [Notice("error", f"coinbase error: {data.get('message')} {data.get('reason')}")]
        return [Notice("info", f"ignored coinbase message type {kind!r}")]

    def _trade(self, data: dict[str, Any], recv_us: int, *, backfilled: bool = False) -> Trade:
        maker_side = data.get("side")
        if maker_side not in _OPPOSITE:
            raise AdapterError(f"side is not buy/sell: {maker_side!r}")
        try:
            exchange_ts = iso_to_us(str_field(data, "time"))
        except ValueError as exc:
            raise AdapterError(str(exc)) from None
        return Trade(
            source=self.name,
            symbol=str(data.get("product_id") or ""),
            trade_id=int_field(data.get("trade_id"), "trade_id"),
            price=decimal_field(data.get("price"), "price"),
            size=decimal_field(data.get("size"), "size"),
            side=_OPPOSITE[maker_side],
            exchange_ts_us=exchange_ts,
            recv_ts_us=recv_us,
            backfilled=backfilled,
        )

    def backfill(
        self,
        symbol: str,
        first_missing_id: int,
        last_missing_id: int,
        *,
        get: HttpGet,
        recv_us: Callable[[], int],
        max_trades: int,
    ) -> list[Trade]:
        found: dict[int, Trade] = {}
        cursor: str | None = str(last_missing_id + 1)
        product = urllib.parse.quote(symbol, safe="")
        while cursor is not None and len(found) < max_trades:
            query = urllib.parse.urlencode({"after": cursor, "limit": 1000})
            body, headers = get(f"{self.rest_url}/products/{product}/trades?{query}")
            if not isinstance(body, list) or not body:
                break
            fetched_at = recv_us()
            lowest = None
            for item in body:
                if not isinstance(item, dict):
                    raise AdapterError("trades item is not an object")
                trade = self._trade({**item, "product_id": symbol}, fetched_at, backfilled=True)
                trade_id = trade.trade_id or 0
                lowest = trade_id if lowest is None else min(lowest, trade_id)
                if first_missing_id <= trade_id <= last_missing_id:
                    found[trade_id] = trade
            if lowest is None or lowest <= first_missing_id:
                break
            cursor = _header(headers, "cb-after")
            if cursor is not None and self.page_pause_s:
                time.sleep(self.page_pause_s)
        ordered = sorted(found.values(), key=lambda t: t.trade_id or 0)
        return ordered[:max_trades]


def _header(headers: Any, name: str) -> str | None:
    for key, value in dict(headers).items():
        if str(key).lower() == name:
            return str(value)
    return None
