"""Command line: ``mdrec record | verify | bars | stats``."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
from collections.abc import Sequence
from pathlib import Path

from market_data_recorder import __version__
from market_data_recorder.adapters import ADAPTERS, get_adapter
from market_data_recorder.rebuild import rebuild_bars
from market_data_recorder.recorder import Recorder, RecorderConfig
from market_data_recorder.stats import compute_stats, format_stats
from market_data_recorder.timeutil import parse_interval, parse_size
from market_data_recorder.verify import format_report, verify

DESCRIPTION = (
    "Record public, unauthenticated market-data WebSocket feeds into gap-aware, checksummed\n"
    "CSV segments that tickreplay reads without conversion. No API keys, no account\n"
    "endpoints, no orders. Recorded data is not investment information."
)


def _seconds(text: str) -> float:
    return float(parse_interval(text)) if text[-1:].isalpha() else float(text)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mdrec",
        description=DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    rec = sub.add_parser("record", help="record a public trade stream until Ctrl+C")
    rec.add_argument("--source", required=True, choices=sorted(ADAPTERS))
    rec.add_argument(
        "--symbols",
        required=True,
        help="comma-separated, in the source's spelling (BTCUSDT / BTC-USD / BTC/USD)",
    )
    rec.add_argument("--out", type=Path, required=True, help="output directory")
    rec.add_argument(
        "--bars",
        default="1m",
        help="comma-separated bar intervals built live, e.g. 1m,5m; 'none' disables (default 1m)",
    )
    rec.add_argument("--rotate-size", default="64MiB", help="rotate segments at this size")
    rec.add_argument("--rotate-time", default="1h", help="rotate segments at this age")
    rec.add_argument(
        "--duration", type=_seconds, default=None, help="stop after this long (e.g. 300 or 5m)"
    )
    rec.add_argument(
        "--max-skew",
        type=float,
        default=5.0,
        help="seconds of |receive - exchange time| before a clock_skew record (default 5)",
    )
    rec.add_argument(
        "--backfill",
        action="store_true",
        help="fill trade-id gaps from the source's public REST endpoint (binance, coinbase)",
    )
    rec.add_argument("--backfill-max", type=int, default=10_000, help="max trades per gap")
    rec.add_argument("--url", default=None, help="override the WebSocket URL (testing)")
    rec.add_argument("-v", "--verbose", action="store_true", help="debug logging")

    ver = sub.add_parser("verify", help="check checksums, ordering, tickreplay rules and gaps")
    ver.add_argument("path", type=Path)
    ver.add_argument(
        "--salvage", action="store_true", help="recover whole rows from unfinished .part files"
    )
    ver.add_argument("--json", action="store_true", help="machine-readable output")

    bars = sub.add_parser("bars", help="rebuild OHLCV bars from recorded trades")
    bars.add_argument("path", type=Path)
    bars.add_argument("--interval", default="1m", help="e.g. 30s, 1m, 15m, 1h (default 1m)")
    bars.add_argument("--out", type=Path, default=None, help="default: <path>/rebuilt")

    st = sub.add_parser("stats", help="trade rate, exchange->local latency, gap totals")
    st.add_argument("path", type=Path)
    st.add_argument("--json", action="store_true", help="machine-readable output")
    return parser


def _record(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    adapter = get_adapter(args.source)
    symbols = [s for s in (x.strip() for x in args.symbols.split(",")) if s]
    intervals = (
        []
        if args.bars.strip().lower() == "none"
        else [parse_interval(x) for x in args.bars.split(",") if x.strip()]
    )
    config = RecorderConfig(
        source=args.source,
        symbols=symbols,
        out=args.out,
        bar_intervals=intervals,
        rotate_bytes=parse_size(args.rotate_size),
        rotate_seconds=_seconds(args.rotate_time),
        max_skew_s=args.max_skew,
        backfill=args.backfill,
        backfill_max_trades=args.backfill_max,
        url=args.url,
        duration_s=args.duration,
    )
    if args.backfill and not adapter.supports_backfill:
        print(
            f"note: {adapter.name} has no documented public backfill; --backfill ignored",
            file=sys.stderr,
        )
    recorder = Recorder(config, adapter)
    interrupted = False

    async def main() -> None:
        if sys.platform != "win32":
            loop = asyncio.get_running_loop()
            loop.add_signal_handler(signal.SIGTERM, recorder.stop)
        await recorder.run()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        interrupted = True
    recorder.close()
    s = recorder.summary
    print(
        f"{'interrupted; ' if interrupted else ''}flushed and closed {args.out}: "
        f"{sum(s.trades.values())} trades, {sum(s.bars.values())} bars, "
        f"{sum(s.gaps.values())} gap record(s), {s.backfilled} backfilled, "
        f"{s.duplicates} duplicate/late id(s) dropped, {s.malformed} malformed message(s), "
        f"{recorder.feed_stats.connects} connect(s)",
        file=sys.stderr,
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "record":
            return _record(args)
        if args.command == "verify":
            report = verify(args.path, salvage=args.salvage)
            print(json.dumps(report.as_dict(), indent=1) if args.json else format_report(report))
            return 0 if report.ok else 1
        if args.command == "bars":
            files = rebuild_bars(args.path, parse_interval(args.interval), args.out)
            for f in files:
                print(
                    f"{f.source} {f.symbol}: {f.bars} bar(s) ({f.partial} partial) "
                    f"from {f.trades} trade(s) -> {f.path}"
                )
            if not files:
                print("no recorded trades found")
            return 0
        stats = compute_stats(args.path)
        print(json.dumps(stats.as_dict(), indent=1) if args.json else format_stats(stats))
        return 0
    except (ValueError, OSError) as exc:
        print(f"mdrec: error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
