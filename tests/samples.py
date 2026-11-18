"""Sample messages written by hand from each source's documented schema.

None of these were captured from a live feed: ids, prices and times are invented, and the
field layout follows the documentation cited in docs/sources.md.
"""

from __future__ import annotations

import json

from market_data_recorder.timeutil import iso_to_us, us_to_iso

# 2026-01-01T00:00:00Z
T0_US = iso_to_us("2026-01-01T00:00:00Z")


def binance_trade(
    trade_id: int,
    price: str,
    qty: str,
    ts_us: int,
    *,
    symbol: str = "BTCUSDT",
    buyer_is_maker: bool = False,
    micro: bool = False,
) -> str:
    """``<symbol>@trade`` payload (spot WebSocket streams docs, "Trade Streams")."""
    t = ts_us if micro else ts_us // 1000
    return json.dumps(
        {
            "e": "trade",
            "E": t,
            "s": symbol,
            "t": trade_id,
            "p": price,
            "q": qty,
            "T": t,
            "m": buyer_is_maker,
            "M": True,
        }
    )


BINANCE_SUB_ACK = json.dumps({"result": None, "id": 1})


def coinbase_match(
    trade_id: int,
    price: str,
    size: str,
    ts_us: int,
    *,
    maker_side: str = "sell",
    product: str = "BTC-USD",
    kind: str = "match",
    sequence: int = 50,
) -> str:
    """``matches`` channel message (Exchange WebSocket channels docs)."""
    return json.dumps(
        {
            "type": kind,
            "trade_id": trade_id,
            "sequence": sequence,
            "maker_order_id": "ac928c66-ca53-498f-9c13-a110027a60e8",
            "taker_order_id": "132fb6ae-456b-4654-b4e0-d681ac05cea1",
            "time": us_to_iso(ts_us),
            "product_id": product,
            "size": size,
            "price": price,
            "side": maker_side,
        }
    )


def coinbase_heartbeat(last_trade_id: int, ts_us: int, product: str = "BTC-USD") -> str:
    return json.dumps(
        {
            "type": "heartbeat",
            "sequence": 90,
            "last_trade_id": last_trade_id,
            "product_id": product,
            "time": us_to_iso(ts_us),
        }
    )


COINBASE_SUBSCRIPTIONS = json.dumps(
    {
        "type": "subscriptions",
        "channels": [
            {"name": "matches", "product_ids": ["BTC-USD"]},
            {"name": "heartbeat", "product_ids": ["BTC-USD"]},
        ],
    }
)


def kraken_trades(
    trades: list[tuple[int, str, str, int, str]], symbol: str = "BTC/USD", kind: str = "update"
) -> str:
    """``trade`` channel update (Kraken WebSocket v2 docs). Numbers are JSON numbers.

    Each tuple is ``(trade_id, price, qty, ts_us, side)``; price/qty are inserted verbatim
    as JSON number literals so the exact digits can be checked after parsing.
    """
    items = [
        (
            f'{{"symbol": "{symbol}", "side": "{side}", "price": {price}, "qty": {qty}, '
            f'"ord_type": "market", "trade_id": {tid}, "timestamp": "{us_to_iso(ts)}"}}'
        )
        for tid, price, qty, ts, side in trades
    ]
    return f'{{"channel": "trade", "type": "{kind}", "data": [{", ".join(items)}]}}'


KRAKEN_SUB_ACK = json.dumps(
    {
        "method": "subscribe",
        "result": {"channel": "trade", "snapshot": False, "symbol": "BTC/USD"},
        "success": True,
        "time_in": "2026-01-01T00:00:00.428340Z",
        "time_out": "2026-01-01T00:00:00.428375Z",
    }
)
KRAKEN_HEARTBEAT = json.dumps({"channel": "heartbeat"})
