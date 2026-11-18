from __future__ import annotations

from decimal import Decimal

import pytest

from market_data_recorder.gaps import GapTracker
from market_data_recorder.models import GapRecord, Trade
from market_data_recorder.timeutil import (
    format_interval,
    iso_to_us,
    parse_interval,
    parse_size,
    us_to_compact,
    us_to_iso,
    us_to_iso_seconds,
)
from samples import T0_US

# -- time helpers ---------------------------------------------------------------------


def test_t0_is_the_hand_computed_epoch() -> None:
    # 2026-01-01 is 20454 days after 1970-01-01 (56 years incl. 14 leap days):
    # 56 * 365 + 14 = 20454; 20454 * 86400 = 1_767_225_600 s.
    assert T0_US == 1_767_225_600 * 1_000_000


@pytest.mark.parametrize(
    ("text", "offset_us"),
    [
        ("2026-01-01T00:00:00Z", 0),
        ("2026-01-01T00:00:00.5Z", 500_000),
        ("2026-01-01T00:00:00.123456789Z", 123_456),  # nanoseconds truncated
        ("2026-01-01 00:00:01", 1_000_000),  # naive -> UTC
        ("2026-01-01T01:00:00+01:00", 0),
        ("2025-12-31T19:00:00-05:00", 0),
        ("2026-01-01T05:30:00+0530", 0),
    ],
)
def test_iso_to_us(text: str, offset_us: int) -> None:
    assert iso_to_us(text) == T0_US + offset_us


def test_iso_round_trip_and_formats() -> None:
    us = T0_US + 3_723_000_042  # 01:02:03.000042
    assert us_to_iso(us) == "2026-01-01T01:02:03.000042Z"
    assert iso_to_us(us_to_iso(us)) == us
    assert us_to_iso_seconds(us) == "2026-01-01T01:02:03Z"
    assert us_to_compact(us) == "20260101T010203Z"
    with pytest.raises(ValueError):
        iso_to_us("01/01/2026")


def test_intervals_and_sizes() -> None:
    assert [parse_interval(x) for x in ("30s", "1m", "15m", "4h", "1d")] == [
        30,
        60,
        900,
        14400,
        86400,
    ]
    assert [format_interval(x) for x in (30, 60, 900, 14400, 86400, 90)] == [
        "30s",
        "1m",
        "15m",
        "4h",
        "1d",
        "90s",
    ]
    for bad in ("1x", "m", "0m"):
        with pytest.raises(ValueError):
            parse_interval(bad)
    assert parse_size("512KB") == 512_000
    assert parse_size("64MiB") == 64 * 1024 * 1024
    assert parse_size("100") == 100
    for bad in ("0", "1TB", "-1"):
        with pytest.raises(ValueError):
            parse_size(bad)


# -- gap tracker ----------------------------------------------------------------------


def trade(tid: int | None, sec: float, *, latency_s: float = 0.1) -> Trade:
    ts = T0_US + int(sec * 1_000_000)
    return Trade(
        "binance",
        "BTCUSDT",
        tid,
        Decimal("100"),
        Decimal("1"),
        "buy",
        ts,
        ts + int(latency_s * 1_000_000),
    )


def feed(tracker: GapTracker, trades: list[Trade]) -> list[GapRecord]:
    records: list[GapRecord] = []
    for t in trades:
        _, recs = tracker.on_trade(t)
        records.extend(recs)
    return records


def test_contiguous_ids_produce_no_records() -> None:
    tracker = GapTracker("binance", "BTCUSDT")
    assert feed(tracker, [trade(i, i) for i in range(1, 6)]) == []
    assert tracker.last_id == 5


def test_sequence_jump_reports_exact_missing_range() -> None:
    tracker = GapTracker("binance", "BTCUSDT")
    records = feed(tracker, [trade(1, 1), trade(2, 2), trade(6, 7)])
    (gap,) = records
    assert gap.reason == "sequence_jump"
    assert (gap.first_missing_id, gap.last_missing_id, gap.missing) == (3, 5, 3)
    assert (gap.start_us, gap.end_us) == (T0_US + 2_000_000, T0_US + 7_000_000)
    assert gap.loses_data


def test_disconnect_with_contiguous_ids_records_zero_loss() -> None:
    tracker = GapTracker("binance", "BTCUSDT")
    feed(tracker, [trade(1, 1), trade(2, 2)])
    tracker.on_disconnect(T0_US + 3_000_000, "ConnectionClosed")
    assert tracker.disconnected
    (gap,) = feed(tracker, [trade(3, 9)])
    assert gap.reason == "disconnect"
    assert gap.missing == 0 and gap.first_missing_id is None
    assert not gap.loses_data
    assert (gap.start_us, gap.end_us) == (T0_US + 2_000_000, T0_US + 9_000_000)
    assert "no trades lost" in gap.detail
    assert not tracker.disconnected


def test_disconnect_with_lost_trades() -> None:
    tracker = GapTracker("binance", "BTCUSDT")
    feed(tracker, [trade(10, 1)])
    tracker.on_disconnect(T0_US + 2_000_000, "reset")
    tracker.on_disconnect(T0_US + 2_500_000, "again")  # second call keeps the first window
    (gap,) = feed(tracker, [trade(14, 5)])
    assert (gap.reason, gap.first_missing_id, gap.last_missing_id, gap.missing) == (
        "disconnect",
        11,
        13,
        3,
    )
    assert "reset" in gap.detail


def test_disconnect_without_ids_is_unknown_loss() -> None:
    tracker = GapTracker("x", "S")
    feed(tracker, [trade(None, 1)])
    tracker.on_disconnect(T0_US + 2_000_000, "eof")
    (gap,) = feed(tracker, [trade(None, 4)])
    assert gap.missing is None and gap.loses_data
    assert "no way to count" in gap.detail


def test_duplicates_dropped_and_regressions_recorded() -> None:
    tracker = GapTracker("binance", "BTCUSDT")
    feed(tracker, [trade(5, 1)])
    accept, records = tracker.on_trade(trade(5, 1))
    assert not accept and records == []
    accept, records = tracker.on_trade(trade(3, 2))
    assert not accept
    assert records[0].reason == "out_of_order" and "dropped" in records[0].detail
    assert tracker.duplicates == 2


def test_replayed_ids_after_reconnect_close_the_disconnect() -> None:
    tracker = GapTracker("binance", "BTCUSDT")
    feed(tracker, [trade(5, 1)])
    tracker.on_disconnect(T0_US + 2_000_000, "x")
    accept, records = tracker.on_trade(trade(5, 1))
    assert not accept
    assert records[0].reason == "disconnect" and records[0].missing == 0


def test_backwards_exchange_time_is_kept_and_recorded() -> None:
    tracker = GapTracker("binance", "BTCUSDT")
    feed(tracker, [trade(1, 10)])
    accept, records = tracker.on_trade(trade(2, 9))
    assert accept
    assert records[0].reason == "out_of_order"
    assert tracker.last_exchange_us == T0_US + 10_000_000


def test_clock_skew_window() -> None:
    tracker = GapTracker("binance", "BTCUSDT", max_skew_us=1_000_000)
    records = feed(
        tracker,
        [
            trade(1, 1),
            trade(2, 2, latency_s=-1.5),  # local clock 1.5 s behind the exchange
            trade(3, 3, latency_s=-2.5),
            trade(4, 4),  # back within tolerance: window closes
        ],
    )
    (gap,) = records
    assert gap.reason == "clock_skew"
    assert (gap.start_us, gap.end_us) == (T0_US + 2_000_000, T0_US + 3_000_000)
    assert "2 trade(s)" in gap.detail and "-2.500 s" in gap.detail
    assert not gap.loses_data


def test_heartbeat_reveals_missed_trades() -> None:
    tracker = GapTracker("coinbase", "BTC-USD")
    assert tracker.on_heartbeat(None, T0_US) == []
    assert tracker.on_heartbeat(100, T0_US) == []  # baseline
    assert tracker.on_heartbeat(100, T0_US + 1) == []
    (gap,) = tracker.on_heartbeat(103, T0_US + 2_000_000)
    assert (gap.reason, gap.first_missing_id, gap.last_missing_id, gap.missing) == (
        "sequence_jump",
        101,
        103,
        3,
    )
    # The next live trade continues from the heartbeat's id without a second record.
    accept, records = tracker.on_trade(trade(104, 3))
    assert accept and records == []


def test_heartbeat_closes_disconnect() -> None:
    tracker = GapTracker("coinbase", "BTC-USD")
    feed(tracker, [trade(50, 1)])
    tracker.on_disconnect(T0_US + 2_000_000, "idle")
    (gap,) = tracker.on_heartbeat(52, T0_US + 5_000_000)
    assert (gap.reason, gap.missing, gap.first_missing_id) == ("disconnect", 2, 51)


def test_close_flushes_open_windows() -> None:
    tracker = GapTracker("binance", "BTCUSDT", max_skew_us=1)
    feed(tracker, [trade(1, 1, latency_s=3)])
    tracker.on_disconnect(T0_US + 5_000_000, "eof")
    records = tracker.close(T0_US + 9_000_000)
    assert [r.reason for r in records] == ["disconnect", "clock_skew"]
    assert "recording stopped" in records[0].detail
    assert records[0].end_us == T0_US + 9_000_000
    assert tracker.close(T0_US + 10_000_000) == []


def test_gap_record_json_round_trip() -> None:
    tracker = GapTracker("binance", "BTCUSDT", id_prefix="abc")
    (gap,) = feed(tracker, [trade(1, 1), trade(3, 2)])
    assert gap.gap_id == "abc-BTCUSDT-1"
    import json

    data = json.loads(gap.to_json())
    assert data["type"] == "gap" and data["start"] == "2026-01-01T00:00:01.000000Z"
    assert GapRecord.from_dict(data) == gap


def test_trade_validation() -> None:
    with pytest.raises(ValueError):
        Trade("s", "x", 1, Decimal("-1"), Decimal(1), "", 0, 0)
    with pytest.raises(ValueError):
        Trade("s", "x", 1, Decimal(1), Decimal("NaN"), "", 0, 0)
