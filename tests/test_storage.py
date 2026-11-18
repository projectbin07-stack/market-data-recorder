from __future__ import annotations

import hashlib
import itertools
import json
from decimal import Decimal
from pathlib import Path

import pytest

from market_data_recorder.csvformat import TRADE_COLUMNS, header_row, trade_row
from market_data_recorder.models import Trade
from market_data_recorder.storage import (
    MANIFEST,
    Manifest,
    SegmentWriter,
    csv_header,
    find_parts,
    parse_header_tag,
    salvage_part,
    scan_segment,
    sha256_file,
    symbol_dir,
)
from samples import T0_US

S = 1_000_000


class Clock:
    def __init__(self, start: int = T0_US) -> None:
        self.now = start

    def __call__(self) -> int:
        return self.now


def make_trade(tid: int, sec: float = 0.0) -> Trade:
    ts = T0_US + int(sec * S)
    return Trade("binance", "BTCUSDT", tid, Decimal("100.25"), Decimal("0.5"), "buy", ts, ts + 7)


def writer(root: Path, manifest: Manifest, clock: Clock, **kw: object) -> SegmentWriter:
    params: dict[str, object] = {
        "kind": "trades",
        "source": "binance",
        "symbol": "BTCUSDT",
        "directory": Path("binance") / "BTCUSDT",
        "prefix": "trades",
        "header": csv_header("trades", "binance", "BTCUSDT", "s1") + header_row(TRADE_COLUMNS),
        "session": "s1",
        "rotate_bytes": 10_000,
        "rotate_seconds": 3600.0,
        "clock": clock,
    }
    params.update(kw)
    return SegmentWriter(root, manifest, **params)  # type: ignore[arg-type]


def test_symbol_dir_is_filesystem_safe() -> None:
    assert symbol_dir("BTC/USD") == "BTC_USD"
    assert symbol_dir("BTC-USD") == "BTC-USD"
    assert symbol_dir("") == "_"


def test_rotation_by_size_writes_atomic_checksummed_segments(tmp_path: Path) -> None:
    clock = Clock()
    manifest = Manifest(tmp_path)
    w = writer(tmp_path, manifest, clock, rotate_bytes=600)
    for i in range(1, 21):
        w.write(trade_row(make_trade(i, i)), T0_US + i * S, i, i)
    w.finalize()
    reloaded = Manifest.load(tmp_path)
    assert len(reloaded.segments) >= 3
    assert sum(s.rows for s in reloaded.segments) == 20
    ids = [(s.first_trade_id, s.last_trade_id) for s in reloaded.segments]
    assert ids[0][0] == 1 and ids[-1][1] == 20
    for (_, last), (first, _) in itertools.pairwise(ids):
        assert first == (last or 0) + 1  # nothing lost between segments
    for seg in reloaded.segments:
        path = tmp_path / seg.path
        assert path.suffix == ".csv"
        assert sha256_file(path) == seg.sha256
        assert path.stat().st_size == seg.bytes
        assert seg.bytes <= 600 + 200  # rotation happens right after crossing the limit
    assert find_parts(tmp_path) == []


def test_rotation_by_time(tmp_path: Path) -> None:
    clock = Clock()
    manifest = Manifest(tmp_path)
    w = writer(tmp_path, manifest, clock, rotate_seconds=60.0)
    w.write(trade_row(make_trade(1)), T0_US, 1, 1)
    clock.now += 59 * S
    w.maybe_rotate(clock.now)
    assert manifest.segments == []
    clock.now += 1 * S
    w.maybe_rotate(clock.now)
    assert len(manifest.segments) == 1
    w.maybe_rotate(clock.now + 3600 * S)  # nothing open: no-op
    assert len(manifest.segments) == 1


def test_timestamp_regression_starts_a_new_segment(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path)
    w = writer(tmp_path, manifest, Clock())
    w.write(trade_row(make_trade(1, 10)), T0_US + 10 * S, 1, 1)
    w.write(trade_row(make_trade(2, 10)), T0_US + 10 * S, 2, 2)  # equal is fine
    w.write(trade_row(make_trade(3, 9)), T0_US + 9 * S, 3, 3)  # backwards: rotate
    w.finalize()
    assert [s.rows for s in manifest.segments] == [2, 1]
    # Two segments opened in the same second still get distinct names.
    assert len({s.path for s in manifest.segments}) == 2


def test_strict_timestamps_rotate_on_equal(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path)
    w = writer(tmp_path, manifest, Clock(), strict_ts=True)
    w.write("a\n", 5)
    w.write("b\n", 5)
    w.finalize()
    assert [s.rows for s in manifest.segments] == [1, 1]


def test_empty_segment_leaves_nothing(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path)
    w = writer(tmp_path, manifest, Clock())
    assert w.finalize() is None
    w._open()
    assert w.part_path is not None and w.part_path.exists()
    assert w.finalize() is None
    assert find_parts(tmp_path) == [] and manifest.segments == []


def test_part_file_holds_flushed_rows_until_finalize(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path)
    w = writer(tmp_path, manifest, Clock())
    w.write(trade_row(make_trade(1)), T0_US, 1, 1)
    w.flush()
    part = w.part_path
    assert part is not None and part.name.endswith(".csv.part")
    assert b"100.25" in part.read_bytes()
    assert not (tmp_path / MANIFEST).exists()
    entry = w.finalize()
    assert entry is not None and not part.exists()
    assert (tmp_path / entry.path).exists()


def test_manifest_round_trip_and_format_check(tmp_path: Path) -> None:
    assert Manifest.load(tmp_path).segments == []
    (tmp_path / MANIFEST).write_text(json.dumps({"format": "other"}), encoding="utf-8")
    with pytest.raises(ValueError, match="unknown manifest format"):
        Manifest.load(tmp_path)


def test_header_tag_parsing() -> None:
    header = csv_header("bars", "kraken", "BTC/USD", "abc", "interval_s=60")
    tag = parse_header_tag(header.splitlines()[0])
    assert tag == {
        "kind": "bars",
        "source": "kraken",
        "symbol": "BTC/USD",
        "session": "abc",
        "interval_s": "60",
    }
    assert parse_header_tag("# something else") == {}


def _crash_part(tmp_path: Path, rows: int, tail: bytes) -> Path:
    manifest = Manifest(tmp_path)
    w = writer(tmp_path, manifest, Clock())
    w._open()
    for i in range(1, rows + 1):
        w.write(trade_row(make_trade(i, i)), T0_US + i * S, i, i)
    w.flush()
    assert w._handle is not None
    w._handle.write(tail)
    w._handle.flush()
    w._handle.close()  # simulate the process dying: never finalised
    assert w.part_path is not None
    return w.part_path


def test_salvage_keeps_whole_rows_and_drops_torn_tail(tmp_path: Path) -> None:
    part = _crash_part(tmp_path, 5, b"2026-01-01T00:00:06.000000Z,BTCUSDT,100.")
    manifest = Manifest.load(tmp_path)
    entry = salvage_part(tmp_path, part, manifest)
    assert entry is not None
    assert entry.recovered and entry.rows == 5
    assert (entry.first_trade_id, entry.last_trade_id) == (1, 5)
    final = tmp_path / entry.path
    assert not part.exists() and final.exists()
    assert hashlib.sha256(final.read_bytes()).hexdigest() == entry.sha256
    assert scan_segment(final, "trades").rows == 5
    assert Manifest.load(tmp_path).segments[-1].recovered


def test_salvage_stops_at_first_bad_complete_row(tmp_path: Path) -> None:
    part = _crash_part(tmp_path, 3, b"garbage,row\n2026-01-01T00:00:09.000000Z,x\n")
    entry = salvage_part(tmp_path, part, Manifest.load(tmp_path))
    assert entry is not None and entry.rows == 3


def test_salvage_of_header_only_part_removes_it(tmp_path: Path) -> None:
    part = _crash_part(tmp_path, 0, b"")
    assert salvage_part(tmp_path, part, Manifest.load(tmp_path)) is None
    assert not part.exists()


def test_salvage_rejects_foreign_files(tmp_path: Path) -> None:
    part = tmp_path / "x.csv.part"
    part.write_bytes(b"a,b\n1,2\n")
    with pytest.raises(ValueError, match="not a market-data-recorder segment"):
        salvage_part(tmp_path, part, Manifest(tmp_path))


def test_salvage_gap_segment(tmp_path: Path) -> None:
    folder = tmp_path / "binance"
    folder.mkdir()
    part = folder / "gaps-x.jsonl.part"
    header = json.dumps({"type": "header", "kind": "gaps", "source": "binance", "session": "s"})
    record = json.dumps({"type": "gap", "detected_us": 5})
    part.write_text(f'{header}\n{record}\n{{"type": "ga', encoding="utf-8")
    entry = salvage_part(tmp_path, part, Manifest(tmp_path))
    assert entry is not None and entry.kind == "gaps" and entry.rows == 1
    assert entry.symbol is None and entry.source == "binance"
