"""``mdrec bars``: rebuild OHLCV bars offline from recorded trade segments."""

from __future__ import annotations

import os
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from market_data_recorder.bars import build_bars
from market_data_recorder.csvformat import BAR_COLUMNS, bar_row, header_row, read_trades
from market_data_recorder.models import Trade
from market_data_recorder.storage import DISCLAIMER, Manifest, replace_with_retry, symbol_dir
from market_data_recorder.timeutil import format_interval
from market_data_recorder.verify import REBUILT_DIR, load_gap_files


@dataclass(frozen=True)
class RebuiltFile:
    path: Path
    source: str
    symbol: str
    bars: int
    partial: int
    trades: int


def load_trades(root: Path, manifest: Manifest) -> dict[tuple[str, str], list[Trade]]:
    """All recorded trades per stream, live and backfilled merged, deduplicated by id."""
    by_stream: dict[tuple[str, str], dict[object, Trade]] = defaultdict(dict)
    for entry in manifest.segments:
        if entry.kind != "trades" or entry.symbol is None:
            continue
        bucket = by_stream[(entry.source, entry.symbol)]
        for trade in read_trades(root / entry.path, entry.source):
            key: object = (
                trade.trade_id
                if trade.trade_id is not None
                else (
                    trade.exchange_ts_us,
                    trade.price,
                    trade.size,
                    len(bucket),
                )
            )
            bucket.setdefault(key, trade)
    return {
        key: sorted(trades.values(), key=lambda t: (t.exchange_ts_us, t.trade_id or 0))
        for key, trades in by_stream.items()
    }


def rebuild_bars(root: Path, interval_s: int, out: Path | None = None) -> list[RebuiltFile]:
    """Write ``<out>/<source>/<symbol>/bars-<interval>.csv`` for every recorded stream."""
    manifest = Manifest.load(root)
    out = out or root / REBUILT_DIR
    gaps, _ = load_gap_files(root, manifest.segments)
    label = format_interval(interval_s)
    written: list[RebuiltFile] = []
    for (source, symbol), trades in sorted(load_trades(root, manifest).items()):
        # Id-bearing gaps are found again from id continuity in the merged trades (so a
        # gap fully repaired by backfill does not mark bars partial); gaps without ids
        # can only be known from their records.
        stream_gaps = [
            g
            for g in gaps
            if g.source == source and g.symbol == symbol and g.missing is None and g.loses_data
        ]
        bars = build_bars(trades, interval_s, gaps=stream_gaps)
        folder = out / source / symbol_dir(symbol)
        folder.mkdir(parents=True, exist_ok=True)
        target = folder / f"bars-{label}.csv"
        tmp = target.with_name(target.name + ".tmp")
        with tmp.open("w", encoding="utf-8", newline="") as handle:
            handle.write(
                f"# market-data-recorder kind=bars source={source} symbol={symbol} "
                f"interval_s={interval_s} rebuilt=1\n# {DISCLAIMER}\n"
                f"# {label} OHLCV bars rebuilt from {len(trades)} recorded trade(s); "
                "timestamp=bar open time; partial=1 means the bar may be missing trades\n"
            )
            handle.write(header_row(BAR_COLUMNS))
            for bar in bars:
                handle.write(bar_row(bar))
            handle.flush()
            os.fsync(handle.fileno())
        replace_with_retry(tmp, target)
        written.append(
            RebuiltFile(
                target, source, symbol, len(bars), sum(b.partial for b in bars), len(trades)
            )
        )
    return written
