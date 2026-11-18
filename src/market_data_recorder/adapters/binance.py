"""Binance spot public trade streams (``<symbol>@trade``).

Documentation: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
Relevant published rules (see docs/sources.md):

- the server pings every 20 s and drops the connection if no pong arrives within a minute
  (the WebSocket client answers pings automatically);
- at most 5 incoming messages per second per connection, *including* ping/pong frames and
  SUBSCRIBE requests, so all symbols are subscribed in one request and client pings are
  sparse;
- at most 1024 streams per connection and 300 connection attempts per 5 minutes per IP;
- a connection is valid for 24 hours and then closed by the server (handled as a normal
  reconnect, with a gap record).

Backfill uses ``GET /api/v3/historicalTrades`` (``fromId``; no API key required) on the
market-data-only host ``data-api.binance.vision``.
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
from market_data_recorder.models import Event, Notice, Trade


class BinanceAdapter(SourceAdapter):
    name: ClassVar[str] = "binance"
    default_url: ClassVar[str] = "wss://stream.binance.com:9443/ws"
    rest_url: ClassVar[str] = "https://data-api.binance.vision"
    # One client ping per 30 s keeps us far below the 5 messages/s budget.
    ping_interval: ClassVar[float | None] = 30.0
    ping_timeout: ClassVar[float | None] = 30.0
    # Quiet symbols can go minutes without a trade; liveness comes from ping/pong.
    idle_timeout: ClassVar[float | None] = None
    max_symbols: ClassVar[int] = 1024
    supports_backfill: ClassVar[bool] = True
    #: Pause between REST pages (each historicalTrades call has request weight 25).
    page_pause_s: ClassVar[float] = 0.5

    def subscribe_messages(self, symbols: Sequence[str]) -> list[str]:
        params = [f"{s.lower()}@trade" for s in symbols]
        return [json.dumps({"method": "SUBSCRIBE", "params": params, "id": 1})]

    def parse(self, raw: str | bytes, recv_us: int) -> list[Event]:
        data = load_json(raw)
        if not isinstance(data, dict):
            raise AdapterError("expected a JSON object")
        if "stream" in data and isinstance(data.get("data"), dict):
            data = data["data"]  # combined-stream wrapper
        if "id" in data and "result" in data:
            return [Notice("ack", f"request {data['id']} ok")]
        if "code" in data or "error" in data:
            error = data.get("error", data)
            return [Notice("error", f"binance error: {error}")]
        if data.get("e") != "trade":
            return [Notice("info", f"ignored binance event {data.get('e')!r}")]
        return [self._trade(data, recv_us)]

    def _trade(self, data: dict[str, Any], recv_us: int) -> Trade:
        buyer_is_maker = data.get("m")
        if not isinstance(buyer_is_maker, bool):
            raise AdapterError("trade field 'm' is missing or not a boolean")
        trade_time = int_field(data.get("T"), "T")
        return Trade(
            source=self.name,
            symbol=str_field(data, "s").upper(),
            trade_id=int_field(data.get("t"), "t"),
            price=decimal_field(data.get("p"), "p"),
            size=decimal_field(data.get("q"), "q"),
            # Buyer is maker -> the aggressor (taker) sold.
            side="sell" if buyer_is_maker else "buy",
            exchange_ts_us=_ms_or_us_to_us(trade_time),
            recv_ts_us=recv_us,
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
        wanted = min(last_missing_id - first_missing_id + 1, max_trades)
        trades: list[Trade] = []
        next_id = first_missing_id
        while len(trades) < wanted:
            limit = min(1000, wanted - len(trades))
            query = urllib.parse.urlencode({"symbol": symbol, "fromId": next_id, "limit": limit})
            body, _headers = get(f"{self.rest_url}/api/v3/historicalTrades?{query}")
            if not isinstance(body, list) or not body:
                break
            fetched_at = recv_us()
            for item in body:
                if not isinstance(item, dict):
                    raise AdapterError("historicalTrades item is not an object")
                trade_id = int_field(item.get("id"), "id")
                if trade_id < next_id or trade_id > last_missing_id:
                    continue
                trades.append(
                    Trade(
                        source=self.name,
                        symbol=symbol,
                        trade_id=trade_id,
                        price=decimal_field(item.get("price"), "price"),
                        size=decimal_field(item.get("qty"), "qty"),
                        side="sell" if item.get("isBuyerMaker") else "buy",
                        exchange_ts_us=_ms_or_us_to_us(int_field(item.get("time"), "time")),
                        recv_ts_us=fetched_at,
                        backfilled=True,
                    )
                )
            last_seen = int_field(body[-1].get("id"), "id")
            if last_seen >= last_missing_id or len(body) < limit:
                break
            next_id = last_seen + 1
            if self.page_pause_s:
                time.sleep(self.page_pause_s)
        trades.sort(key=lambda t: t.trade_id or 0)
        return trades[:wanted]


def _ms_or_us_to_us(value: int) -> int:
    """Binance sends milliseconds by default and microseconds with ``timeUnit=MICROSECOND``.

    Millisecond epochs stay below 1e14 until the year 5138; microsecond epochs are above
    1e15 since 2001, so the magnitude identifies the unit.
    """
    return value * 1000 if value < 10**14 else value
