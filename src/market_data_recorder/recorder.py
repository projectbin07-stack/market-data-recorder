"""The recorder: wires a source adapter, gap tracking, bar building and storage together."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import secrets
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from market_data_recorder import __version__
from market_data_recorder.adapters.base import (
    AdapterError,
    HttpGet,
    SourceAdapter,
    http_get_json,
)
from market_data_recorder.bars import BarBuilder
from market_data_recorder.connection import Backoff, FeedStats, run_feed
from market_data_recorder.csvformat import (
    BAR_COLUMNS,
    TRADE_COLUMNS,
    bar_row,
    header_row,
    trade_row,
)
from market_data_recorder.gaps import GapTracker
from market_data_recorder.models import Bar, GapRecord, Heartbeat, Notice, Trade
from market_data_recorder.storage import (
    DISCLAIMER,
    Manifest,
    SegmentWriter,
    csv_header,
    find_parts,
    symbol_dir,
)
from market_data_recorder.timeutil import format_interval, now_us

log = logging.getLogger("market_data_recorder.recorder")


@dataclass
class RecorderConfig:
    source: str
    symbols: Sequence[str]
    out: Path
    bar_intervals: Sequence[int] = (60,)
    rotate_bytes: int = 64 * 1024 * 1024
    rotate_seconds: float = 3600.0
    max_skew_s: float = 5.0
    bar_grace_s: float = 2.0
    flush_interval_s: float = 1.0
    backfill: bool = False
    backfill_max_trades: int = 10_000
    url: str | None = None
    duration_s: float | None = None
    idle_timeout_s: float | None = None
    backoff_base_s: float = 1.0
    backoff_cap_s: float = 60.0
    stable_after_s: float = 30.0


@dataclass
class RecorderSummary:
    trades: Counter[str] = field(default_factory=Counter)
    bars: Counter[str] = field(default_factory=Counter)
    gaps: Counter[str] = field(default_factory=Counter)
    duplicates: int = 0
    malformed: int = 0
    unknown_symbol: int = 0
    errors: list[str] = field(default_factory=list)
    backfilled: int = 0
    late_trades: int = 0


class _SymbolState:
    def __init__(self, tracker: GapTracker, trades: SegmentWriter) -> None:
        self.tracker = tracker
        self.trades = trades
        self.builders: list[tuple[BarBuilder, SegmentWriter]] = []
        self.backfill_writer: SegmentWriter | None = None


class Recorder:
    """Record one source's trades for a set of symbols until stopped."""

    def __init__(
        self,
        config: RecorderConfig,
        adapter: SourceAdapter,
        *,
        clock: Callable[[], int] = now_us,
        http_get: HttpGet = http_get_json,
        backoff: Backoff | None = None,
        connect: Any = None,
    ) -> None:
        if not config.symbols:
            raise ValueError("at least one symbol is required")
        if len(config.symbols) > adapter.max_symbols:
            raise ValueError(
                f"{adapter.name} allows at most {adapter.max_symbols} symbols per connection"
            )
        self.config = config
        self.adapter = adapter
        self.clock = clock
        self.http_get = http_get
        self.backoff = backoff or Backoff(config.backoff_base_s, config.backoff_cap_s)
        self.connect = connect
        self.session = secrets.token_hex(3)
        self.root = config.out
        self.root.mkdir(parents=True, exist_ok=True)
        self.leftover_parts = find_parts(self.root)
        self.manifest = Manifest.load(self.root)
        self.summary = RecorderSummary()
        self.feed_stats = FeedStats()
        self.symbols = [adapter.normalize_symbol(s) for s in config.symbols]
        self._states: dict[str, _SymbolState] = {}
        self._stop = asyncio.Event()
        self._backfill_queue: asyncio.Queue[GapRecord] | None = None
        self._closed = False
        self.gap_writer = SegmentWriter(
            self.root,
            self.manifest,
            kind="gaps",
            source=adapter.name,
            symbol=None,
            directory=Path(adapter.name),
            prefix="gaps",
            header=json.dumps(
                {
                    "type": "header",
                    "kind": "gaps",
                    "source": adapter.name,
                    "session": self.session,
                    "writer": f"market-data-recorder {__version__}",
                    "note": DISCLAIMER,
                },
                sort_keys=True,
            )
            + "\n",
            session=self.session,
            rotate_bytes=config.rotate_bytes,
            rotate_seconds=config.rotate_seconds,
            clock=clock,
        )
        for symbol in self.symbols:
            self._states[symbol] = self._make_state(symbol)

    # -- setup ---------------------------------------------------------------------
    def _writer(self, symbol: str, kind: str, prefix: str, header: str, **kw: Any) -> SegmentWriter:
        return SegmentWriter(
            self.root,
            self.manifest,
            kind="trades" if kind == "trades" else "bars",
            source=self.adapter.name,
            symbol=symbol,
            directory=Path(self.adapter.name) / symbol_dir(symbol),
            prefix=prefix,
            header=header,
            session=self.session,
            rotate_bytes=self.config.rotate_bytes,
            rotate_seconds=self.config.rotate_seconds,
            clock=self.clock,
            **kw,
        )

    def _trade_header(self, symbol: str, backfill: bool) -> str:
        extra = "backfill=1" if backfill else ""
        return (
            csv_header("trades", self.adapter.name, symbol, self.session, extra)
            + "# timestamp=exchange trade time (UTC); recv_timestamp=local receive time; "
            "side=taker side\n" + header_row(TRADE_COLUMNS)
        )

    def _make_state(self, symbol: str) -> _SymbolState:
        tracker = GapTracker(
            self.adapter.name,
            symbol,
            max_skew_us=int(self.config.max_skew_s * 1_000_000),
            id_prefix=self.session,
        )
        trades = self._writer(symbol, "trades", "trades", self._trade_header(symbol, False))
        state = _SymbolState(tracker, trades)
        for interval in self.config.bar_intervals:
            label = format_interval(interval)
            header = (
                csv_header(
                    "bars", self.adapter.name, symbol, self.session, f"interval_s={interval}"
                )
                + f"# {label} OHLCV bars built locally from recorded trades; timestamp=bar open "
                "time; partial=1 means the bar may be missing trades\n" + header_row(BAR_COLUMNS)
            )
            writer = self._writer(
                symbol, "bars", f"bars-{label}", header, interval_s=interval, strict_ts=True
            )
            state.builders.append((BarBuilder(self.adapter.name, symbol, interval), writer))
        return state

    # -- event handling ------------------------------------------------------------
    def on_message(self, raw: str | bytes, recv_us: int) -> None:
        try:
            events = self.adapter.parse(raw, recv_us)
        except (AdapterError, ValueError, ArithmeticError) as exc:
            self.summary.malformed += 1
            log.warning("%s: unparseable message (%s): %.200r", self.adapter.name, exc, raw)
            return
        for event in events:
            if isinstance(event, Trade):
                self._on_trade(event)
            elif isinstance(event, Heartbeat):
                self._on_heartbeat(event, recv_us)
            elif isinstance(event, Notice):
                self._on_notice(event)

    def _state_for(self, symbol: str) -> _SymbolState | None:
        state = self._states.get(symbol)
        if state is None:
            self.summary.unknown_symbol += 1
        return state

    def _on_trade(self, trade: Trade) -> None:
        state = self._state_for(trade.symbol)
        if state is None:
            return
        accept, records = state.tracker.on_trade(trade)
        self._write_gaps(state, records)
        if not accept:
            self.summary.duplicates += 1
            return
        state.trades.write(trade_row(trade), trade.exchange_ts_us, trade.trade_id, trade.trade_id)
        self.summary.trades[trade.symbol] += 1
        for builder, writer in state.builders:
            for bar in builder.add(trade):
                self._write_bar(writer, bar)

    def _on_heartbeat(self, heartbeat: Heartbeat, recv_us: int) -> None:
        if heartbeat.symbol is None:
            return
        state = self._state_for(heartbeat.symbol)
        if state is not None:
            self._write_gaps(state, state.tracker.on_heartbeat(heartbeat.last_trade_id, recv_us))

    def _on_notice(self, notice: Notice) -> None:
        if notice.kind == "error":
            self.summary.errors.append(notice.text)
            log.error("%s", notice.text)
        elif notice.kind == "ack":
            log.info("%s: %s", self.adapter.name, notice.text)
        else:
            log.debug("%s: %s", self.adapter.name, notice.text)

    def on_state(self, connected: bool, reason: str) -> None:
        if connected:
            return
        now = self.clock()
        for state in self._states.values():
            state.tracker.on_disconnect(now, reason)
            for builder, _ in state.builders:
                builder.note_disconnect()

    def _write_bar(self, writer: SegmentWriter, bar: Bar) -> None:
        writer.write(bar_row(bar), bar.open_ts_us, bar.first_trade_id, bar.last_trade_id)
        self.summary.bars[bar.symbol] += 1

    def _write_gaps(self, state: _SymbolState, records: list[GapRecord]) -> None:
        for record in records:
            self.gap_writer.write(record.to_json() + "\n", record.detected_us)
            self.gap_writer.flush()
            self.summary.gaps[record.reason] += 1
            log.warning(
                "%s %s: %s gap %s .. %s missing=%s (%s)",
                record.source,
                record.symbol,
                record.reason,
                record.start_us,
                record.end_us,
                record.missing,
                record.detail,
            )
            for builder, _ in state.builders:
                builder.note_gap(record)
            if (
                self.config.backfill
                and self.adapter.supports_backfill
                and self._backfill_queue is not None
                and record.first_missing_id is not None
                and record.last_missing_id is not None
            ):
                self._backfill_queue.put_nowait(record)

    # -- backfill --------------------------------------------------------------------
    async def _backfill_worker(self) -> None:
        assert self._backfill_queue is not None
        while True:
            record = await self._backfill_queue.get()
            assert record.first_missing_id is not None and record.last_missing_id is not None
            try:
                trades = await asyncio.to_thread(
                    self.adapter.backfill,
                    record.symbol,
                    record.first_missing_id,
                    record.last_missing_id,
                    get=self.http_get,
                    recv_us=self.clock,
                    max_trades=self.config.backfill_max_trades,
                )
                error = ""
            except (OSError, ValueError, ArithmeticError) as exc:
                trades, error = [], f"{type(exc).__name__}: {exc}"
            self._write_backfill(record, trades, error)

    def _write_backfill(self, record: GapRecord, trades: list[Trade], error: str) -> None:
        state = self._states[record.symbol]
        if trades and state.backfill_writer is None:
            state.backfill_writer = self._writer(
                record.symbol,
                "trades",
                "trades-backfill",
                self._trade_header(record.symbol, True),
                backfill=True,
            )
        for trade in trades:
            assert state.backfill_writer is not None
            state.backfill_writer.write(
                trade_row(trade), trade.exchange_ts_us, trade.trade_id, trade.trade_id
            )
        if state.backfill_writer is not None:
            state.backfill_writer.flush()
        requested = (record.last_missing_id or 0) - (record.first_missing_id or 0) + 1
        detected = self.clock()
        result: dict[str, object] = {
            "type": "backfill",
            "gap_id": record.gap_id,
            "source": record.source,
            "symbol": record.symbol,
            "requested": requested,
            "received": len(trades),
            "complete": len(trades) == requested,
            "first_trade_id": trades[0].trade_id if trades else None,
            "last_trade_id": trades[-1].trade_id if trades else None,
            "error": error,
            "detected_us": detected,
        }
        self.gap_writer.write(json.dumps(result, sort_keys=True) + "\n", detected)
        self.gap_writer.flush()
        self.summary.backfilled += len(trades)
        log.info(
            "%s %s: backfilled %d of %d trades for gap %s%s",
            record.source,
            record.symbol,
            len(trades),
            requested,
            record.gap_id,
            f" ({error})" if error else "",
        )

    # -- periodic work and lifecycle ---------------------------------------------------
    def tick(self) -> None:
        """Flush to disk, close bars whose interval has passed, rotate by age."""
        now = self.clock()
        grace = int(self.config.bar_grace_s * 1_000_000)
        for state in self._states.values():
            for builder, writer in state.builders:
                for bar in builder.close_due(now, grace):
                    self._write_bar(writer, bar)
                writer.flush()
                writer.maybe_rotate(now)
            state.trades.flush()
            state.trades.maybe_rotate(now)
            if state.backfill_writer is not None:
                state.backfill_writer.maybe_rotate(now)
        self.gap_writer.flush()
        self.gap_writer.maybe_rotate(now)

    def stop(self) -> None:
        self._stop.set()

    async def _ticker(self) -> None:
        while True:
            await asyncio.sleep(self.config.flush_interval_s)
            self.tick()

    async def run(self) -> RecorderSummary:
        """Record until :meth:`stop`, the configured duration, or cancellation (Ctrl+C)."""
        for part in self.leftover_parts:
            log.warning(
                "unfinished segment from an earlier run: %s (run `mdrec verify --salvage`)", part
            )
        self._backfill_queue = asyncio.Queue()
        kwargs: dict[str, Any] = {}
        if self.connect is not None:
            kwargs["connect"] = self.connect
        tasks = [
            asyncio.create_task(
                run_feed(
                    self.adapter,
                    self.symbols,
                    url=self.config.url,
                    on_message=self.on_message,
                    on_state=self.on_state,
                    backoff=self.backoff,
                    stable_after=self.config.stable_after_s,
                    idle_timeout=self.config.idle_timeout_s,
                    stats=self.feed_stats,
                    **kwargs,
                )
            ),
            asyncio.create_task(self._ticker()),
            asyncio.create_task(self._backfill_worker()),
        ]
        try:
            stop_wait = asyncio.create_task(self._stop.wait())
            done, _ = await asyncio.wait(
                [stop_wait, *tasks],
                timeout=self.config.duration_s,
                return_when=asyncio.FIRST_COMPLETED,
            )
            stop_wait.cancel()
            for task in done:
                error = None if task is stop_wait else task.exception()
                if error is not None:
                    raise error
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            self.close()
        return self.summary

    def close(self) -> None:
        """Flush everything and finalise every open segment. Safe to call twice."""
        if self._closed:
            return
        self._closed = True
        now = self.clock()
        for state in self._states.values():
            self._write_gaps(state, state.tracker.close(now))
            for builder, writer in state.builders:
                for bar in builder.flush():
                    self._write_bar(writer, bar)
                self.summary.late_trades += builder.late_trades
                writer.finalize()
            state.trades.finalize()
            if state.backfill_writer is not None:
                state.backfill_writer.finalize()
        self.gap_writer.finalize()
