"""OHLCV bars built locally from trades.

A bar covers ``[open, open + interval)`` with ``open`` aligned to the Unix epoch, so 1m
bars open on whole minutes. It is emitted only once its interval has closed: when a trade
from a later interval arrives, or when the wall clock passes the close plus a grace period
(:meth:`BarBuilder.close_due`). Intervals without trades produce no bar.

A bar is marked ``partial`` when it may be missing trades: the first bar of a recording
session (trades before the start were not seen), a bar overlapping a gap record that lost
or may have lost data, and a bar flushed at shutdown before its interval closed.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from decimal import Decimal

from market_data_recorder.models import Bar, GapRecord, Trade

US = 1_000_000


@dataclass
class _Open:
    open_ts_us: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trades: int
    partial: bool
    first_id: int | None
    last_id: int | None


@dataclass
class BarBuilder:
    """Aggregate one symbol's trades into fixed-interval bars."""

    source: str
    symbol: str
    interval_s: int
    late_trades: int = 0
    _current: _Open | None = None
    _session_start: bool = True
    _last_emitted_open: int | None = None
    _gaps: list[tuple[int, int]] = field(default_factory=list)
    _disconnected_since: int | None = None

    @property
    def interval_us(self) -> int:
        return self.interval_s * US

    def bucket(self, ts_us: int) -> int:
        return ts_us - ts_us % self.interval_us

    def note_gap(self, record: GapRecord) -> None:
        """Remember a gap window so bars overlapping it are marked partial."""
        if record.loses_data:
            self._gaps.append((record.start_us, record.end_us))
            if self._current is not None and self._overlaps(self._current.open_ts_us):
                self._current.partial = True

    def note_disconnect(self) -> None:
        """The feed dropped; the bar in progress cannot be known to be complete."""
        if self._current is not None:
            self._current.partial = True

    def _overlaps(self, open_us: int) -> bool:
        close_us = open_us + self.interval_us
        return any(start < close_us and end >= open_us for start, end in self._gaps)

    def add(self, trade: Trade) -> list[Bar]:
        """Add a trade; returns bars whose interval closed because of it."""
        bucket = self.bucket(trade.exchange_ts_us)
        emitted: list[Bar] = []
        current = self._current
        if current is not None and bucket < current.open_ts_us:
            self.late_trades += 1
            return emitted
        if (
            current is None
            and self._last_emitted_open is not None
            and bucket <= self._last_emitted_open
        ):
            self.late_trades += 1
            return emitted
        if current is not None and bucket > current.open_ts_us:
            emitted.append(self._emit(current, force_partial=False))
            current = None
        if current is None:
            self._current = _Open(
                open_ts_us=bucket,
                open=trade.price,
                high=trade.price,
                low=trade.price,
                close=trade.price,
                volume=trade.size,
                trades=1,
                partial=self._session_start or self._overlaps(bucket),
                first_id=trade.trade_id,
                last_id=trade.trade_id,
            )
            self._session_start = False
            return emitted
        current.high = max(current.high, trade.price)
        current.low = min(current.low, trade.price)
        current.close = trade.price
        current.volume += trade.size
        current.trades += 1
        if trade.trade_id is not None:
            current.last_id = trade.trade_id
            if current.first_id is None:
                current.first_id = trade.trade_id
        return emitted

    def close_due(self, now_us: int, grace_us: int) -> list[Bar]:
        """Emit the open bar if wall-clock time is past its close plus ``grace_us``."""
        current = self._current
        if current is not None and now_us >= current.open_ts_us + self.interval_us + grace_us:
            return [self._emit(current, force_partial=False)]
        return []

    def flush(self) -> list[Bar]:
        """Emit the open bar at shutdown, marked partial (its interval has not closed)."""
        if self._current is None:
            return []
        return [self._emit(self._current, force_partial=True)]

    def _emit(self, bar: _Open, *, force_partial: bool) -> Bar:
        self._current = None
        self._last_emitted_open = bar.open_ts_us
        partial = bar.partial or force_partial or self._overlaps(bar.open_ts_us)
        # Gaps that ended before this bar can no longer affect later bars.
        self._gaps = [g for g in self._gaps if g[1] >= bar.open_ts_us]
        return Bar(
            source=self.source,
            symbol=self.symbol,
            interval_s=self.interval_s,
            open_ts_us=bar.open_ts_us,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            trades=bar.trades,
            partial=partial,
            first_trade_id=bar.first_id,
            last_trade_id=bar.last_id,
        )


def build_bars(
    trades: Iterable[Trade],
    interval_s: int,
    *,
    gaps: Iterable[GapRecord] = (),
    mark_edges: bool = True,
) -> list[Bar]:
    """Rebuild bars offline from trades sorted by exchange time.

    Besides gap records, a trade-id discontinuity between consecutive trades (for example
    between two recording sessions) marks the bars on both sides as partial. With
    ``mark_edges`` the first and last bar are partial too, because nothing says whether
    trades existed just outside the recorded range.
    """
    trades = list(trades)
    if not trades:
        return []
    first = trades[0]
    builder = BarBuilder(first.source, first.symbol, interval_s)
    builder._session_start = mark_edges
    for record in gaps:
        builder.note_gap(record)
    bars: list[Bar] = []
    previous_id: int | None = None
    for trade in trades:
        jump = (
            previous_id is not None
            and trade.trade_id is not None
            and trade.trade_id > previous_id + 1
        )
        if jump and builder._current is not None:
            # Unrecorded trades between previous_id and trade_id: both sides are incomplete.
            builder._current.partial = True
        bars.extend(builder.add(trade))
        if jump and builder._current is not None:
            builder._current.partial = True
        if trade.trade_id is not None:
            previous_id = trade.trade_id
    tail = builder.flush() if mark_edges else builder.close_due(2**62, 0)
    bars.extend(tail)
    return bars
