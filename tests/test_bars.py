"""Bar building checked against OHLCV worked out by hand."""

from __future__ import annotations

from decimal import Decimal

from market_data_recorder.bars import BarBuilder, build_bars
from market_data_recorder.models import Bar, GapRecord, Trade
from samples import T0_US

S = 1_000_000


def tr(tid: int | None, sec: float, price: str, size: str) -> Trade:
    ts = T0_US + int(sec * S)
    return Trade("binance", "BTCUSDT", tid, Decimal(price), Decimal(size), "buy", ts, ts)


# Minute 0: 100.0 x1, 101.5 x2, 99.5 x0.5, 100.5 x1   -> O 100.0 H 101.5 L 99.5 C 100.5 V 4.5
# Minute 1: 102 x3, 101 x0.25                           -> O 102 H 102 L 101 C 101 V 3.25
# Minute 2: no trades                                   -> no bar
# Minute 3: 98 x1                                       -> O=H=L=C 98, V 1
TRADES = [
    tr(1, 5, "100.0", "1"),
    tr(2, 20, "101.5", "2"),
    tr(3, 40, "99.5", "0.5"),
    tr(4, 59.999999, "100.5", "1"),
    tr(5, 70, "102", "3"),
    tr(6, 119, "101", "0.25"),
    tr(7, 180, "98", "1"),
]


def ohlcv(bar: Bar) -> tuple[str, str, str, str, str, int]:
    return (
        str(bar.open),
        str(bar.high),
        str(bar.low),
        str(bar.close),
        str(bar.volume),
        bar.trades,
    )


def test_live_builder_emits_only_closed_intervals() -> None:
    builder = BarBuilder("binance", "BTCUSDT", 60)
    emitted: list[Bar] = []
    for t in TRADES[:4]:
        emitted += builder.add(t)
    assert emitted == []  # minute 0 still open
    emitted += builder.add(TRADES[4])  # first trade of minute 1 closes minute 0
    (m0,) = emitted
    assert ohlcv(m0) == ("100.0", "101.5", "99.5", "100.5", "4.5", 4)
    assert m0.open_ts_us == T0_US and m0.close_ts_us == T0_US + 60 * S
    assert (m0.first_trade_id, m0.last_trade_id) == (1, 4)
    assert m0.partial  # first bar of the session: earlier trades were not seen
    emitted = builder.add(TRADES[5]) + builder.add(TRADES[6])
    (m1,) = emitted
    assert ohlcv(m1) == ("102", "102", "101", "101", "3.25", 2)
    assert m1.open_ts_us == T0_US + 60 * S
    assert not m1.partial
    (m3,) = builder.flush()
    assert ohlcv(m3) == ("98", "98", "98", "98", "1", 1)
    assert m3.open_ts_us == T0_US + 180 * S
    assert m3.partial  # flushed at shutdown before its interval closed
    assert builder.flush() == []


def test_close_due_uses_grace_period() -> None:
    builder = BarBuilder("binance", "BTCUSDT", 60)
    builder.add(TRADES[0])
    assert builder.close_due(T0_US + 61 * S, 2 * S) == []
    (bar,) = builder.close_due(T0_US + 62 * S, 2 * S)
    assert bar.open_ts_us == T0_US
    # A trade for the already-emitted minute is late: counted, not merged.
    assert builder.add(tr(2, 30, "1", "1")) == []
    assert builder.late_trades == 1


def test_late_trade_into_open_bar_from_earlier_interval() -> None:
    builder = BarBuilder("binance", "BTCUSDT", 60)
    builder.add(tr(1, 70, "1", "1"))
    assert builder.add(tr(2, 10, "1", "1")) == []
    assert builder.late_trades == 1


def test_gap_marks_overlapping_bars_partial_but_zero_loss_gap_does_not() -> None:
    builder = BarBuilder("binance", "BTCUSDT", 60)
    builder._session_start = False
    builder.add(tr(1, 5, "10", "1"))
    lossless = GapRecord(
        "g1", "binance", "BTCUSDT", "disconnect", T0_US + 5 * S, T0_US + 8 * S, 0, missing=0
    )
    builder.note_gap(lossless)
    builder.add(tr(2, 8, "11", "1"))
    lossy = GapRecord(
        "g2", "binance", "BTCUSDT", "sequence_jump", T0_US + 50 * S, T0_US + 65 * S, 0, 3, 5, 3
    )
    builder.note_gap(lossy)
    (m0,) = builder.add(tr(6, 65, "12", "1"))
    assert m0.partial  # overlaps the lossy gap
    (m1,) = builder.add(tr(7, 130, "12", "1"))
    assert m1.partial  # minute 1 also overlaps [50 s, 65 s]
    (m2,) = builder.close_due(T0_US + 200 * S, 0)
    assert not m2.partial


def test_lossless_gap_alone_keeps_bar_complete() -> None:
    builder = BarBuilder("binance", "BTCUSDT", 60)
    builder._session_start = False
    builder.add(tr(1, 5, "10", "1"))
    builder.note_gap(
        GapRecord(
            "g", "binance", "BTCUSDT", "disconnect", T0_US + 5 * S, T0_US + 9 * S, 0, missing=0
        )
    )
    (bar,) = builder.add(tr(2, 65, "10", "1"))
    assert not bar.partial


def test_disconnect_marks_bar_in_progress() -> None:
    builder = BarBuilder("binance", "BTCUSDT", 60)
    builder._session_start = False
    builder.add(tr(1, 5, "10", "1"))
    builder.note_disconnect()
    (bar,) = builder.add(tr(2, 65, "10", "1"))
    assert bar.partial


def test_offline_rebuild_matches_hand_values_and_marks_edges() -> None:
    bars = build_bars(TRADES, 60)
    assert [ohlcv(b) for b in bars] == [
        ("100.0", "101.5", "99.5", "100.5", "4.5", 4),
        ("102", "102", "101", "101", "3.25", 2),
        ("98", "98", "98", "98", "1", 1),
    ]
    assert [b.partial for b in bars] == [True, False, True]
    unmarked = build_bars(TRADES, 60, mark_edges=False)
    assert [b.partial for b in unmarked] == [False, False, False]
    assert build_bars([], 60) == []


def test_offline_rebuild_marks_both_sides_of_an_id_jump() -> None:
    trades = [
        tr(1, 5, "1", "1"),
        tr(2, 65, "1", "1"),
        tr(9, 70, "1", "1"),
        tr(10, 130, "1", "1"),
        tr(11, 190, "1", "1"),
    ]
    bars = build_bars(trades, 60, mark_edges=False)
    # The jump 2 -> 9 happens inside minute 1, so only minute 1 is partial.
    assert [b.partial for b in bars] == [False, True, False, False]
    trades = [tr(1, 5, "1", "1"), tr(5, 65, "1", "1"), tr(6, 130, "1", "1")]
    bars = build_bars(trades, 60, mark_edges=False)
    # The jump 1 -> 5 spans the minute boundary: minutes 0 and 1 are partial.
    assert [b.partial for b in bars] == [True, True, False]


def test_five_minute_alignment() -> None:
    builder = BarBuilder("binance", "BTCUSDT", 300)
    builder.add(tr(1, 299, "5", "1"))
    (bar,) = builder.add(tr(2, 300, "6", "1"))
    assert bar.open_ts_us == T0_US and bar.close == Decimal(5)
