"""Kraken spot WebSocket API v2 public ``trade`` channel.

Documentation: https://docs.kraken.com/api/docs/websocket-v2/trade (plus the heartbeat
and ping pages, and the WebSocket introduction guide). Relevant published rules (see
docs/sources.md):

- the public endpoint is ``wss://ws.kraken.com/v2``; the trade channel needs no token;
- prices and quantities are JSON *numbers*, so messages are decoded with
  ``parse_float=Decimal`` to keep the exact digits that were sent;
- ``trade_id`` is a sequence number unique per book, used for continuity checks;
- a heartbeat arrives about once a second when no other update is sent, and the server
  closes connections that are idle for about a minute;
- about 150 connection attempts per rolling 10 minutes per IP are allowed before a
  10-minute ban, which the reconnect backoff stays well below.

No REST backfill is offered for Kraken: its public ``Trades`` endpoint pages by time
rather than by trade id, so a backfill could not be matched exactly to a gap.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any, ClassVar

from market_data_recorder.adapters.base import (
    AdapterError,
    SourceAdapter,
    decimal_field,
    int_field,
    load_json,
    str_field,
)
from market_data_recorder.models import Event, Heartbeat, Notice, TakerSide, Trade
from market_data_recorder.timeutil import iso_to_us

_SIDES: dict[str, TakerSide] = {"buy": "buy", "sell": "sell"}


class KrakenAdapter(SourceAdapter):
    name: ClassVar[str] = "kraken"
    default_url: ClassVar[str] = "wss://ws.kraken.com/v2"
    ping_interval: ClassVar[float | None] = 20.0
    ping_timeout: ClassVar[float | None] = 20.0
    idle_timeout: ClassVar[float | None] = 10.0
    app_ping_interval: ClassVar[float | None] = 30.0
    max_symbols: ClassVar[int] = 100

    def normalize_symbol(self, symbol: str) -> str:
        return symbol.strip().upper()

    def subscribe_messages(self, symbols: Sequence[str]) -> list[str]:
        message = {
            "method": "subscribe",
            "params": {"channel": "trade", "symbol": list(symbols), "snapshot": False},
            "req_id": 1,
        }
        return [json.dumps(message)]

    def app_ping(self, counter: int) -> str | None:
        return json.dumps({"method": "ping", "req_id": 1000 + counter})

    def parse(self, raw: str | bytes, recv_us: int) -> list[Event]:
        data = load_json(raw, decimals=True)
        if not isinstance(data, dict):
            raise AdapterError("expected a JSON object")
        method = data.get("method")
        if method == "pong":
            return [Notice("pong", f"pong {data.get('req_id')}")]
        if method in ("subscribe", "unsubscribe"):
            if data.get("success") is True:
                return [
                    Notice("ack", f"{method} ok: {json.dumps(data.get('result'), default=str)}")
                ]
            return [Notice("error", f"kraken {method} failed: {data.get('error')}")]
        channel = data.get("channel")
        if channel == "heartbeat":
            return [Heartbeat(None)]
        if channel == "trade" and data.get("type") in ("update", "snapshot"):
            items = data.get("data")
            if not isinstance(items, list):
                raise AdapterError("trade message has no data list")
            return [self._trade(item, recv_us) for item in items]
        if "error" in data:
            return [Notice("error", f"kraken error: {data.get('error')}")]
        return [Notice("info", f"ignored kraken message channel={channel!r} method={method!r}")]

    def _trade(self, item: Any, recv_us: int) -> Trade:
        if not isinstance(item, dict):
            raise AdapterError("trade item is not an object")
        side = item.get("side")
        if side not in _SIDES:
            raise AdapterError(f"side is not buy/sell: {side!r}")
        try:
            exchange_ts = iso_to_us(str_field(item, "timestamp"))
        except ValueError as exc:
            raise AdapterError(str(exc)) from None
        return Trade(
            source=self.name,
            symbol=str_field(item, "symbol"),
            trade_id=int_field(item.get("trade_id"), "trade_id"),
            price=decimal_field(item.get("price"), "price"),
            size=decimal_field(item.get("qty"), "qty"),
            side=_SIDES[side],
            exchange_ts_us=exchange_ts,
            recv_ts_us=recv_us,
        )
