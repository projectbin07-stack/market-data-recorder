"""Recorded files load in tickreplay itself, without conversion.

tickreplay (https://github.com/projectbin07-stack/tickreplay) is installed for the tests
only: locally from its source checkout, in CI from its v0.1.0 tag. It is not a runtime
dependency. If it is missing the tests skip, unless ``MDREC_REQUIRE_TICKREPLAY=1`` (set in
CI) turns that into a failure. The same rules are also re-implemented in
``market_data_recorder.csvformat`` and checked in test_tools.py without tickreplay.
"""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from market_data_recorder.csvformat import check_tickreplay_bars, check_tickreplay_trades
from market_data_recorder.rebuild import rebuild_bars
from market_data_recorder.storage import Manifest
from recording import IDS, make_recording


def _tickreplay() -> Any:
    try:
        from tickreplay.data import csvio
    except ImportError:
        if os.environ.get("MDREC_REQUIRE_TICKREPLAY") == "1":
            raise
        pytest.skip("tickreplay is not installed")
    return csvio


def test_tickreplay_reads_recorded_trades_and_bars(tmp_path: Path) -> None:
    csvio = _tickreplay()
    make_recording(tmp_path, bar_intervals=(60, 120))
    manifest = Manifest.load(tmp_path)
    (trades_seg,) = [s for s in manifest.segments if s.kind == "trades"]
    series = csvio.read_trades(tmp_path / trades_seg.path)
    assert len(series.trades) == len(IDS)
    first = series.trades[0]
    assert first.symbol == "BTCUSDT"
    assert first.price == Decimal("101") and first.size == Decimal("1")
    assert first.ts.isoformat() == "2026-01-01T00:00:05+00:00"
    assert any("Not investment information" in c for c in series.comments)

    bar_segs = sorted(
        (s for s in manifest.segments if s.kind == "bars"), key=lambda s: s.interval_s or 0
    )
    one_minute = csvio.read_bars(tmp_path / bar_segs[0].path)
    assert [(b.open, b.high, b.low, b.close, b.volume) for b in one_minute.bars] == [
        (Decimal(101), Decimal(111), Decimal(101), Decimal(111), Decimal(11)),
        (Decimal(112), Decimal(123), Decimal(112), Decimal(123), Decimal(11)),
        (Decimal(124), Decimal(130), Decimal(124), Decimal(130), Decimal(7)),
    ]
    assert one_minute.gaps == ()  # minutes 0, 1, 2 are consecutive
    two_minute = csvio.read_bars(tmp_path / bar_segs[1].path)
    assert len(two_minute.bars) == 2

    (rebuilt,) = rebuild_bars(tmp_path, 60)
    assert csvio.read_bars(rebuilt.path).bars == one_minute.bars


def test_our_rule_checks_agree_with_tickreplay_on_bad_files(tmp_path: Path) -> None:
    csvio = _tickreplay()
    cases = {
        "trades_backwards.csv": "timestamp,price,size\n2026-01-01T00:00:02Z,1,1\n"
        "2026-01-01T00:00:01Z,1,1\n",
        "trades_zero_price.csv": "timestamp,price,size\n2026-01-01T00:00:01Z,0,1\n",
        "trades_short_row.csv": "timestamp,price,size\n2026-01-01T00:00:01Z,1\n",
        "trades_no_rows.csv": "# only a comment\ntimestamp,price,size\n",
        "trades_ok.csv": "# note\ntimestamp,symbol,price,size,extra\n"
        "2026-01-01T00:00:01.123456Z,X,1.5,0,a\n2026-01-01T00:00:01.123456Z,X,2,1,b\n",
    }
    for name, text in cases.items():
        path = tmp_path / name
        path.write_text(text, encoding="utf-8")
        ours = check_tickreplay_trades(path)
        try:
            csvio.read_trades(path)
            theirs_ok = True
        except csvio.DataValidationError:
            theirs_ok = False
        assert (ours == []) == theirs_ok, (name, ours)

    bar_cases = {
        "bars_dup.csv": "timestamp,open,high,low,close,volume\n"
        "2026-01-01T00:00:00Z,1,1,1,1,1\n2026-01-01T00:00:00Z,1,1,1,1,1\n",
        "bars_high_low.csv": "timestamp,open,high,low,close,volume\n"
        "2026-01-01T00:00:00Z,1,0.5,1,1,1\n",
        "bars_missing_col.csv": "timestamp,open,high,low,close\n2026-01-01T00:00:00Z,1,1,1,1\n",
        "bars_ok.csv": "timestamp,symbol,open,high,low,close,volume,partial\n"
        "2026-01-01T00:00:00Z,X,1,2,0.5,1.5,3,1\n2026-01-01T00:01:00Z,X,1,1,1,1,0,0\n",
    }
    for name, text in bar_cases.items():
        path = tmp_path / name
        path.write_text(text, encoding="utf-8")
        ours = check_tickreplay_bars(path)
        try:
            csvio.read_bars(path)
            theirs_ok = True
        except csvio.DataValidationError:
            theirs_ok = False
        assert (ours == []) == theirs_ok, (name, ours)
