from __future__ import annotations

import json
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

import pytest

from market_data_recorder.adapters import (
    AdapterError,
    BinanceAdapter,
    CoinbaseAdapter,
    KrakenAdapter,
    get_adapter,
)
from market_data_recorder.adapters.base import decimal_field
from market_data_recorder.models import Heartbeat, Notice, Trade
from market_data_recorder.timeutil import us_to_iso
from samples import (
    BINANCE_SUB_ACK,
    COINBASE_SUBSCRIPTIONS,
    KRAKEN_HEARTBEAT,
    KRAKEN_SUB_ACK,
    T0_US,
    binance_trade,
    coinbase_heartbeat,
    coinbase_match,
    kraken_trades,
)

RECV = T0_US + 250_000


def only_trade(events: list[Any]) -> Trade:
    assert len(events) == 1
    assert isinstance(events[0], Trade)
    return events[0]


# -- Binance ------------------------------------------------------------------------


def test_binance_subscribe_is_one_request_for_all_symbols() -> None:
    msgs = BinanceAdapter().subscribe_messages(["BTCUSDT", "ETHUSDT"])
    assert len(msgs) == 1
    assert json.loads(msgs[0]) == {
        "method": "SUBSCRIBE",
        "params": ["btcusdt@trade", "ethusdt@trade"],
        "id": 1,
    }


def test_binance_trade_is_normalised() -> None:
    raw = binance_trade(12345, "0.00100000", "100.5", T0_US + 136_000, buyer_is_maker=True)
    t = only_trade(BinanceAdapter().parse(raw, RECV))
    assert t.source == "binance"
    assert t.symbol == "BTCUSDT"
    assert t.trade_id == 12345
    assert t.price == Decimal("0.00100000")
    assert str(t.price) == "0.00100000"  # digits preserved
    assert t.size == Decimal("100.5")
    assert t.side == "sell"  # buyer was maker, so the taker sold
    assert t.exchange_ts_us == T0_US + 136_000  # milliseconds -> microseconds
    assert t.recv_ts_us == RECV
    assert t.latency_us == 114_000


def test_binance_microsecond_time_unit_and_combined_wrapper() -> None:
    inner = json.loads(binance_trade(7, "1", "2", T0_US + 123_456, micro=True))
    wrapped = json.dumps({"stream": "btcusdt@trade", "data": inner})
    t = only_trade(BinanceAdapter().parse(wrapped, RECV))
    assert t.exchange_ts_us == T0_US + 123_456
    assert t.side == "buy"


def test_binance_control_messages() -> None:
    adapter = BinanceAdapter()
    assert adapter.parse(BINANCE_SUB_ACK, RECV) == [Notice("ack", "request 1 ok")]
    error = adapter.parse(json.dumps({"code": 2, "msg": "Invalid request"}), RECV)
    assert isinstance(error[0], Notice) and error[0].kind == "error"
    other = adapter.parse(json.dumps({"e": "kline"}), RECV)
    assert isinstance(other[0], Notice) and other[0].kind == "info"


@pytest.mark.parametrize(
    "raw",
    [
        "not json",
        "[1, 2]",
        json.dumps({"e": "trade", "s": "BTCUSDT", "t": 1, "p": "1", "q": "1", "T": 1}),
        json.dumps({"e": "trade", "s": "BTCUSDT", "t": "1", "p": "1", "q": "1", "T": 1, "m": 1}),
        json.dumps({"e": "trade", "s": "BTCUSDT", "t": 1, "p": "x", "q": "1", "T": 1, "m": True}),
        json.dumps({"e": "trade", "s": "BTCUSDT", "t": 1, "p": "0", "q": "1", "T": 1, "m": True}),
    ],
)
def test_binance_rejects_malformed(raw: str) -> None:
    with pytest.raises((AdapterError, ValueError)):
        BinanceAdapter().parse(raw, RECV)


class FakeHttp:
    """Serves canned JSON pages and records the URLs requested."""

    def __init__(self, pages: list[tuple[Any, Mapping[str, str]]]) -> None:
        self.pages = list(pages)
        self.urls: list[str] = []

    def __call__(self, url: str) -> tuple[Any, Mapping[str, str]]:
        self.urls.append(url)
        return self.pages.pop(0) if self.pages else ([], {})


def _binance_rest(tid: int) -> dict[str, Any]:
    return {
        "id": tid,
        "price": f"{100 + tid}.00",
        "qty": "0.5",
        "quoteQty": "50",
        "time": (T0_US // 1000) + tid,
        "isBuyerMaker": tid % 2 == 0,
        "isBestMatch": True,
    }


def test_binance_backfill_pages_by_from_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(BinanceAdapter, "page_pause_s", 0.0)
    adapter = BinanceAdapter()
    # Missing 10..12; the endpoint returns from fromId onward.
    http = FakeHttp([([_binance_rest(i) for i in (10, 11, 12)], {})])
    trades = adapter.backfill("BTCUSDT", 10, 12, get=http, recv_us=lambda: 42, max_trades=100)
    assert [t.trade_id for t in trades] == [10, 11, 12]
    assert all(t.backfilled and t.recv_ts_us == 42 for t in trades)
    assert trades[0].side == "sell" and trades[1].side == "buy"
    assert trades[0].exchange_ts_us == T0_US + 10_000
    assert http.urls == [
        "https://data-api.binance.vision/api/v3/historicalTrades?symbol=BTCUSDT&fromId=10&limit=3"
    ]


def test_binance_backfill_multi_page_and_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(BinanceAdapter, "page_pause_s", 0.0)
    first = [_binance_rest(i) for i in range(1, 1001)]
    second = [_binance_rest(i) for i in range(1001, 1201)]
    http = FakeHttp([(first, {}), (second, {})])
    trades = BinanceAdapter().backfill(
        "BTCUSDT", 1, 5000, get=http, recv_us=lambda: 1, max_trades=1200
    )
    assert len(trades) == 1200
    assert trades[-1].trade_id == 1200
    assert "fromId=1001" in http.urls[1] and "limit=200" in http.urls[1]


# -- Coinbase -----------------------------------------------------------------------


def test_coinbase_subscribe_includes_heartbeat() -> None:
    (msg,) = CoinbaseAdapter().subscribe_messages(["BTC-USD", "ETH-USD"])
    assert json.loads(msg) == {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "ETH-USD"],
        "channels": ["matches", "heartbeat"],
    }


def test_coinbase_match_uses_taker_side() -> None:
    raw = coinbase_match(10, "400.23", "5.23512", T0_US + 28_459, maker_side="sell")
    t = only_trade(CoinbaseAdapter().parse(raw, RECV))
    assert (t.symbol, t.trade_id, t.price, t.size) == (
        "BTC-USD",
        10,
        Decimal("400.23"),
        Decimal("5.23512"),
    )
    assert t.side == "buy"  # maker sold -> taker bought (an up-tick)
    assert t.exchange_ts_us == T0_US + 28_459


def test_coinbase_last_match_and_heartbeat_are_id_references() -> None:
    adapter = CoinbaseAdapter()
    last = adapter.parse(coinbase_match(9, "1", "1", T0_US, kind="last_match"), RECV)
    assert last == [Heartbeat("BTC-USD", 9)]
    assert adapter.parse(coinbase_heartbeat(20, T0_US), RECV) == [Heartbeat("BTC-USD", 20)]


def test_coinbase_control_messages() -> None:
    adapter = CoinbaseAdapter()
    ack = adapter.parse(COINBASE_SUBSCRIPTIONS, RECV)
    assert isinstance(ack[0], Notice) and ack[0].kind == "ack"
    err = adapter.parse(json.dumps({"type": "error", "message": "Failed", "reason": "x"}), RECV)
    assert isinstance(err[0], Notice) and err[0].kind == "error"
    info = adapter.parse(json.dumps({"type": "ticker"}), RECV)
    assert isinstance(info[0], Notice) and info[0].kind == "info"


@pytest.mark.parametrize(
    "raw",
    [
        coinbase_match(1, "1", "1", T0_US, maker_side="up"),
        json.dumps({"type": "match", "trade_id": 1, "side": "buy", "time": "yesterday"}),
        json.dumps({"type": "heartbeat", "product_id": "BTC-USD", "last_trade_id": "7"}),
        "[]",
    ],
)
def test_coinbase_rejects_malformed(raw: str) -> None:
    with pytest.raises(AdapterError):
        CoinbaseAdapter().parse(raw, RECV)


def _cb_rest(tid: int) -> dict[str, Any]:
    return {
        "trade_id": tid,
        "side": "buy",
        "size": "0.01",
        "price": "100.5",
        "time": us_to_iso(T0_US + tid),
    }


def test_coinbase_backfill_pages_towards_older_trades(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(CoinbaseAdapter, "page_pause_s", 0.0)
    # Missing 101..105. First page (after=106) returns 105..103 newest first with a
    # CB-AFTER cursor; the second returns 102..99 and passes the lower bound.
    http = FakeHttp(
        [
            ([_cb_rest(i) for i in (105, 104, 103)], {"CB-AFTER": "103", "CB-BEFORE": "105"}),
            ([_cb_rest(i) for i in (102, 101, 100, 99)], {"cb-after": "99"}),
        ]
    )
    trades = CoinbaseAdapter().backfill(
        "BTC-USD", 101, 105, get=http, recv_us=lambda: 7, max_trades=100
    )
    assert [t.trade_id for t in trades] == [101, 102, 103, 104, 105]
    assert all(t.backfilled and t.side == "sell" and t.symbol == "BTC-USD" for t in trades)
    assert http.urls[0].endswith("/products/BTC-USD/trades?after=106&limit=1000")
    assert "after=103" in http.urls[1]
    assert len(http.urls) == 2


def test_coinbase_backfill_stops_on_empty_page(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(CoinbaseAdapter, "page_pause_s", 0.0)
    http = FakeHttp([([], {})])
    trades = CoinbaseAdapter().backfill("BTC-USD", 1, 5, get=http, recv_us=lambda: 0, max_trades=9)
    assert trades == []


# -- Kraken -------------------------------------------------------------------------


def test_kraken_subscribe_and_ping() -> None:
    adapter = KrakenAdapter()
    (msg,) = adapter.subscribe_messages(["BTC/USD"])
    assert json.loads(msg) == {
        "method": "subscribe",
        "params": {"channel": "trade", "symbol": ["BTC/USD"], "snapshot": False},
        "req_id": 1,
    }
    assert json.loads(adapter.app_ping(3) or "") == {"method": "ping", "req_id": 1003}


def test_kraken_batched_trades_keep_exact_decimal_digits() -> None:
    raw = kraken_trades(
        [
            (4665906, "0.5117", "40.0", T0_US + 708_706, "sell"),
            (4665907, "0.1", "0.30000000000000004", T0_US + 708_707, "buy"),
            (4665908, "30000", "1", T0_US + 708_708, "buy"),
        ]
    )
    trades = KrakenAdapter().parse(raw, RECV)
    assert [t.trade_id for t in trades if isinstance(t, Trade)] == [4665906, 4665907, 4665908]
    t0, t1, t2 = (t for t in trades if isinstance(t, Trade))
    assert str(t0.price) == "0.5117" and str(t0.size) == "40.0"
    assert str(t1.size) == "0.30000000000000004"  # not rounded through a float
    assert t2.price == Decimal(30000)
    assert t0.side == "sell" and t1.side == "buy"
    assert t0.exchange_ts_us == T0_US + 708_706


def test_kraken_control_messages() -> None:
    adapter = KrakenAdapter()
    assert adapter.parse(KRAKEN_HEARTBEAT, RECV) == [Heartbeat(None)]
    ack = adapter.parse(KRAKEN_SUB_ACK, RECV)
    assert isinstance(ack[0], Notice) and ack[0].kind == "ack"
    fail = adapter.parse(
        json.dumps(
            {"method": "subscribe", "success": False, "error": "Currency pair not supported"}
        ),
        RECV,
    )
    assert isinstance(fail[0], Notice) and fail[0].kind == "error"
    pong = adapter.parse(json.dumps({"method": "pong", "req_id": 1001}), RECV)
    assert pong == [Notice("pong", "pong 1001")]
    status = adapter.parse(json.dumps({"channel": "status", "type": "update", "data": []}), RECV)
    assert isinstance(status[0], Notice) and status[0].kind == "info"
    err = adapter.parse(json.dumps({"error": "bad"}), RECV)
    assert isinstance(err[0], Notice) and err[0].kind == "error"


@pytest.mark.parametrize(
    "raw",
    [
        json.dumps({"channel": "trade", "type": "update", "data": "x"}),
        json.dumps({"channel": "trade", "type": "update", "data": [1]}),
        kraken_trades([(1, "1", "1", T0_US, "hold")]),
        json.dumps(
            {
                "channel": "trade",
                "type": "update",
                "data": [
                    {
                        "symbol": "BTC/USD",
                        "side": "buy",
                        "price": 1,
                        "qty": 1,
                        "trade_id": 1,
                        "timestamp": "nope",
                    }
                ],
            }
        ),
        "3",
    ],
)
def test_kraken_rejects_malformed(raw: str) -> None:
    with pytest.raises(AdapterError):
        KrakenAdapter().parse(raw, RECV)


def test_kraken_has_no_backfill() -> None:
    with pytest.raises(NotImplementedError):
        KrakenAdapter().backfill("BTC/USD", 1, 2, get=FakeHttp([]), recv_us=lambda: 0, max_trades=1)


# -- registry and helpers -----------------------------------------------------------


def test_registry() -> None:
    assert isinstance(get_adapter("Binance"), BinanceAdapter)
    with pytest.raises(ValueError, match="unknown source"):
        get_adapter("nasdaq")


@pytest.mark.parametrize("value", [None, True, 1.5, "nan", "abc"])
def test_decimal_field_rejects(value: object) -> None:
    with pytest.raises(AdapterError):
        decimal_field(value, "x")


def test_decimal_field_accepts() -> None:
    assert decimal_field(3, "x") == Decimal(3)
    assert decimal_field(Decimal("1.10"), "x") == Decimal("1.10")
