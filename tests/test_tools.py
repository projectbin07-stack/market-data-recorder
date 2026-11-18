"""verify / stats / bars on a recording whose contents are known exactly."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from market_data_recorder.cli import main
from market_data_recorder.csvformat import (
    TRADE_COLUMNS,
    check_tickreplay_bars,
    check_tickreplay_trades,
    header_row,
    iter_rows,
    trade_row,
)
from market_data_recorder.models import Trade
from market_data_recorder.rebuild import rebuild_bars
from market_data_recorder.stats import compute_stats, format_stats, percentile
from market_data_recorder.storage import Manifest, SegmentWriter, csv_header, find_parts
from market_data_recorder.verify import format_report, verify
from recording import IDS, S, make_recording
from samples import T0_US, binance_trade


@pytest.fixture
def rec(tmp_path: Path) -> Path:
    make_recording(tmp_path)
    return tmp_path


def test_recorder_summary_counts_everything(tmp_path: Path) -> None:
    recorder = make_recording(tmp_path)
    s = recorder.summary
    assert s.trades["BTCUSDT"] == 29
    assert s.malformed == 1 and s.unknown_symbol == 1
    assert s.errors and "Invalid request" in s.errors[0]
    assert s.gaps == {"disconnect": 1, "sequence_jump": 1}
    assert s.bars["BTCUSDT"] == 3
    # After close, late events (e.g. a message delivered during shutdown) must not reopen
    # a segment, and close() can safely run again.
    recorder.on_message(binance_trade(31, "131", "1", T0_US + 155 * S), T0_US + 155 * S)
    recorder.on_state(False, "late")
    recorder.tick()
    recorder.close()
    assert find_parts(tmp_path) == []
    assert recorder.summary.trades["BTCUSDT"] == 29
    assert verify(tmp_path).ok


def test_recorded_files_follow_tickreplay_rules(rec: Path) -> None:
    manifest = Manifest.load(rec)
    kinds = sorted(s.kind for s in manifest.segments)
    assert kinds == ["bars", "gaps", "trades"]
    for seg in manifest.segments:
        path = rec / seg.path
        if seg.kind == "trades":
            assert check_tickreplay_trades(path) == []
            rows = list(iter_rows(path))
            assert [int(r["trade_id"]) for _, r in rows] == IDS
            first = rows[0][1]
            assert first == {
                "timestamp": "2026-01-01T00:00:05.000000Z",
                "symbol": "BTCUSDT",
                "price": "101",
                "size": "1",
                "side": "buy",
                "trade_id": "1",
                "recv_timestamp": "2026-01-01T00:00:05.050000Z",
                "backfilled": "0",
            }
        elif seg.kind == "bars":
            assert check_tickreplay_bars(path) == []


def test_live_bars_match_hand_computed_values(rec: Path) -> None:
    (bars_seg,) = [s for s in Manifest.load(rec).segments if s.kind == "bars"]
    rows = [r for _, r in iter_rows(rec / bars_seg.path)]
    got = [
        (r["timestamp"], r["open"], r["high"], r["low"], r["close"], r["volume"], r["trades"])
        for r in rows
    ]
    # Minute 0: trades 1-11 (5 s..55 s) -> O 101 H 111 L 101 C 111, 11 trades.
    # Minute 1: trades 12-23 without 21 (60 s..115 s) -> O 112 H 123 L 112 C 123, 11 trades.
    # Minute 2: trades 24-30 (120 s..150 s) -> O 124 H 130 L 124 C 130, 7 trades.
    assert got == [
        ("2026-01-01T00:00:00Z", "101", "111", "101", "111", "11", "11"),
        ("2026-01-01T00:01:00Z", "112", "123", "112", "123", "11", "11"),
        ("2026-01-01T00:02:00Z", "124", "130", "124", "130", "7", "7"),
    ]
    # Minute 0: session start. Minute 1: overlaps the lost trade 21 (100 s..110 s).
    # Minute 2: flushed at shutdown before 00:03:00.
    assert [r["partial"] for r in rows] == ["1", "1", "1"]


def test_verify_reports_ok_and_gap_totals(rec: Path) -> None:
    report = verify(rec)
    assert report.ok, report.findings
    assert report.segments == 3
    summary = report.gap_summary()
    assert summary.count == {"disconnect": 1, "sequence_jump": 1}
    assert summary.missing == {"disconnect": 0, "sequence_jump": 1}
    # sequence_jump spans trade 20 (100 s) to trade 22 (110 s).
    assert summary.duration_us["sequence_jump"] == 10 * S
    text = format_report(report)
    assert "sequence_jump" in text and text.endswith("OK")


def test_verify_detects_tampering(rec: Path) -> None:
    manifest = Manifest.load(rec)
    (trades,) = [s for s in manifest.segments if s.kind == "trades"]
    path = rec / trades.path
    data = path.read_bytes().replace(b",101,", b",109,", 1)
    path.write_bytes(data)
    report = verify(rec)
    assert not report.ok
    assert any("SHA-256" in f.message for f in report.errors)
    path.unlink()
    assert any("missing on disk" in f.message for f in verify(rec).errors)


def test_verify_detects_unreported_id_jump_and_untracked_files(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path)
    writer = SegmentWriter(
        tmp_path,
        manifest,
        kind="trades",
        source="binance",
        symbol="BTCUSDT",
        directory=Path("binance/BTCUSDT"),
        prefix="trades",
        header=csv_header("trades", "binance", "BTCUSDT", "s") + header_row(TRADE_COLUMNS),
        session="s",
        rotate_bytes=1 << 20,
        rotate_seconds=3600,
    )
    for tid in (1, 2, 5):
        t = Trade(
            "binance", "BTCUSDT", tid, Decimal(1), Decimal(1), "buy", T0_US + tid, T0_US + tid
        )
        writer.write(trade_row(t), t.exchange_ts_us, tid, tid)
    writer.finalize()
    (tmp_path / "binance" / "stray.csv").write_text("x\n", encoding="utf-8")
    (tmp_path / "binance" / "BTCUSDT" / "trades-x.csv.part").write_text("x\n", encoding="utf-8")
    report = verify(tmp_path)
    assert any("jump 2 -> 5 with no gap record" in f.message for f in report.errors)
    warnings = [f.message for f in report.findings if f.level == "warning"]
    assert any("not listed in the manifest" in w for w in warnings)
    assert any("unfinished segment" in w for w in warnings)
    # A .part without a recorder header cannot be salvaged.
    assert any("could not salvage" in f.message for f in verify(tmp_path, salvage=True).errors)


def test_verify_without_manifest(tmp_path: Path) -> None:
    report = verify(tmp_path)
    assert not report.ok and "no manifest" in report.errors[0].message


def test_stats_hand_values(rec: Path) -> None:
    stats = compute_stats(rec)
    (stream,) = stats.streams
    assert stream.trades == 29
    # Receive times run from 5.05 s to 150.05 s: 145 s for 29 trades = 0.2 trades/s.
    assert stream.span_s == pytest.approx(145.0)
    assert stream.rate == pytest.approx(0.2)
    assert stream.latency() == {"p50": 50.0, "p90": 50.0, "p99": 50.0, "min": 50.0, "max": 50.0}
    assert stream.negative_latency == 0
    assert stats.gaps.missing["sequence_jump"] == 1
    text = format_stats(stats)
    assert "not investment information" in text
    assert json.loads(json.dumps(stats.as_dict()))["streams"][0]["trades"] == 29


def test_percentile_nearest_rank() -> None:
    values = [float(v) for v in range(1, 11)]  # 1..10
    # Nearest rank: ceil(p/100 * n). p50 -> rank 5 -> 5; p90 -> rank 9 -> 9; p99 -> rank 10.
    assert [percentile(values, p) for p in (50, 90, 99, 100)] == [5.0, 9.0, 10.0, 10.0]
    assert percentile([7.0], 50) == 7.0
    with pytest.raises(ValueError):
        percentile([], 50)
    with pytest.raises(ValueError):
        percentile(values, 0)


def test_rebuild_bars_matches_live_bars_and_marks_partials(rec: Path) -> None:
    (built,) = rebuild_bars(rec, 60)
    assert built.bars == 3 and built.trades == 29
    rows = [r for _, r in iter_rows(built.path)]
    assert [(r["open"], r["close"], r["volume"]) for r in rows] == [
        ("101", "111", "11"),
        ("112", "123", "11"),
        ("124", "130", "7"),
    ]
    # Edges are partial; minute 1 has the id jump 20 -> 22.
    assert [r["partial"] for r in rows] == ["1", "1", "1"]
    assert check_tickreplay_bars(built.path) == []
    assert verify(rec).ok  # rebuilt/ is outside the manifest by design
    (two,) = rebuild_bars(rec, 120, rec / "other")
    rows = [r for _, r in iter_rows(two.path)]
    # 2-minute bars: [0, 120) has trades 1-23 without 21, [120, 240) has 24-30.
    assert [(r["open"], r["high"], r["trades"]) for r in rows] == [
        ("101", "123", "22"),
        ("124", "130", "7"),
    ]


def test_cli_tools(rec: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["verify", str(rec)]) == 0
    assert capsys.readouterr().out.strip().endswith("OK")
    assert main(["verify", str(rec), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True
    assert main(["stats", str(rec)]) == 0
    assert "trades/s           0.20" in capsys.readouterr().out
    assert main(["stats", str(rec), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["streams"][0]["latency_ms"]["p50"] == 50.0
    assert main(["bars", str(rec), "--interval", "1m"]) == 0
    assert "3 bar(s) (3 partial) from 29 trade(s)" in capsys.readouterr().out
    assert main(["bars", str(rec), "--interval", "bad"]) == 2
    assert main(["verify", str(rec / "nothing")]) == 1


def test_cli_bars_on_empty_dir(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["bars", str(tmp_path)]) == 0
    assert "no recorded trades" in capsys.readouterr().out


def test_cli_version_and_help(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 0
    assert "mdrec 0.1.0" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        main(["record", "--help"])
    assert "--symbols" in capsys.readouterr().out
