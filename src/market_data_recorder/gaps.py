"""Per-symbol continuity tracking that turns every hole into an explicit record.

The tracker never repairs anything. It decides whether a trade is accepted (only exact
duplicates and trade-id regressions are refused, and even those are recorded) and emits
:class:`GapRecord` objects for:

- ``disconnect``: the connection dropped. The window runs from the last trade before the
  drop to the first trade (or id-bearing heartbeat) after it, and when the source has
  trade ids the record says exactly how many trades were lost - possibly zero;
- ``sequence_jump``: the trade id advanced by more than one on a live connection, or a
  heartbeat reported a newer last trade id than the last trade received;
- ``clock_skew``: the exchange timestamp and the local receive time disagreed by more
  than the configured tolerance. Data is kept; the window is recorded when it ends;
- ``out_of_order``: a trade id at or below the last one (dropped), or an exchange
  timestamp earlier than the previous trade's (kept).
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field

from market_data_recorder.models import GapReason, GapRecord, Trade


@dataclass
class _Disconnect:
    since_us: int
    reason: str


@dataclass
class _Skew:
    start_us: int
    end_us: int
    worst_us: int
    count: int


@dataclass
class GapTracker:
    """Continuity state for one ``(source, symbol)`` stream."""

    source: str
    symbol: str
    max_skew_us: int = 5_000_000
    id_prefix: str = "g"
    last_id: int | None = None
    last_exchange_us: int | None = None
    duplicates: int = 0
    _disconnect: _Disconnect | None = None
    _skew: _Skew | None = None
    _counter: itertools.count[int] = field(default_factory=lambda: itertools.count(1))

    # -- record construction -------------------------------------------------------
    def _record(
        self,
        reason: GapReason,
        start_us: int,
        end_us: int,
        detected_us: int,
        *,
        first_missing: int | None = None,
        last_missing: int | None = None,
        missing: int | None = None,
        detail: str = "",
    ) -> GapRecord:
        return GapRecord(
            gap_id=f"{self.id_prefix}-{self.symbol}-{next(self._counter)}",
            source=self.source,
            symbol=self.symbol,
            reason=reason,
            start_us=min(start_us, end_us),
            end_us=max(start_us, end_us),
            detected_us=detected_us,
            first_missing_id=first_missing,
            last_missing_id=last_missing,
            missing=missing,
            detail=detail,
        )

    def _close_disconnect(self, end_us: int, new_last_id: int | None, now_us: int) -> GapRecord:
        assert self._disconnect is not None
        info = self._disconnect
        self._disconnect = None
        start = self.last_exchange_us if self.last_exchange_us is not None else info.since_us
        first = last = missing = None
        if self.last_id is not None and new_last_id is not None:
            missing = max(0, new_last_id - self.last_id)
            if missing:
                first, last = self.last_id + 1, new_last_id
        detail = f"connection lost: {info.reason}"
        if missing == 0:
            detail += "; trade ids are contiguous across the reconnect, no trades lost"
        elif missing is None:
            detail += "; source gives no way to count lost trades"
        return self._record(
            "disconnect",
            start,
            end_us,
            now_us,
            first_missing=first,
            last_missing=last,
            missing=missing,
            detail=detail,
        )

    # -- events --------------------------------------------------------------------
    def on_disconnect(self, now_us: int, reason: str) -> None:
        """The connection carrying this symbol went away."""
        if self._disconnect is None:
            self._disconnect = _Disconnect(now_us, reason)

    @property
    def disconnected(self) -> bool:
        return self._disconnect is not None

    def on_heartbeat(self, last_trade_id: int | None, now_us: int) -> list[GapRecord]:
        """A heartbeat (or Coinbase ``last_match``) reporting the exchange's last trade id."""
        if last_trade_id is None:
            return []
        records: list[GapRecord] = []
        if self.last_id is None:
            # First reference point: nothing to compare against yet.
            self.last_id = last_trade_id
            return records
        if self._disconnect is not None:
            records.append(self._close_disconnect(now_us, last_trade_id, now_us))
        elif last_trade_id > self.last_id:
            start = self.last_exchange_us if self.last_exchange_us is not None else now_us
            records.append(
                self._record(
                    "sequence_jump",
                    start,
                    now_us,
                    now_us,
                    first_missing=self.last_id + 1,
                    last_missing=last_trade_id,
                    missing=last_trade_id - self.last_id,
                    detail="heartbeat reports trades that were never received",
                )
            )
        if last_trade_id > self.last_id:
            self.last_id = last_trade_id
        return records

    def on_trade(self, trade: Trade) -> tuple[bool, list[GapRecord]]:
        """Check one live trade. Returns ``(accept, records)``."""
        records: list[GapRecord] = []
        tid = trade.trade_id
        if tid is not None and self.last_id is not None and tid <= self.last_id:
            # A reconnect that replays already-seen ids, or a genuinely late print.
            if self._disconnect is not None:
                records.append(
                    self._close_disconnect(trade.exchange_ts_us, self.last_id, trade.recv_ts_us)
                )
            self.duplicates += 1
            if tid < self.last_id:
                records.append(
                    self._record(
                        "out_of_order",
                        trade.exchange_ts_us,
                        trade.exchange_ts_us,
                        trade.recv_ts_us,
                        detail=f"trade id {tid} arrived after id {self.last_id}; dropped",
                    )
                )
            return False, records

        if self._disconnect is not None:
            new_last = tid - 1 if tid is not None else None
            records.append(self._close_disconnect(trade.exchange_ts_us, new_last, trade.recv_ts_us))
        elif tid is not None and self.last_id is not None and tid > self.last_id + 1:
            start = (
                self.last_exchange_us if self.last_exchange_us is not None else trade.exchange_ts_us
            )
            records.append(
                self._record(
                    "sequence_jump",
                    start,
                    trade.exchange_ts_us,
                    trade.recv_ts_us,
                    first_missing=self.last_id + 1,
                    last_missing=tid - 1,
                    missing=tid - self.last_id - 1,
                    detail=f"trade id jumped from {self.last_id} to {tid}",
                )
            )

        if self.last_exchange_us is not None and trade.exchange_ts_us < self.last_exchange_us:
            records.append(
                self._record(
                    "out_of_order",
                    trade.exchange_ts_us,
                    self.last_exchange_us,
                    trade.recv_ts_us,
                    detail="exchange timestamp went backwards; trade kept, segment rotated",
                )
            )

        records.extend(self._check_skew(trade))
        if tid is not None:
            self.last_id = tid
        if self.last_exchange_us is None or trade.exchange_ts_us > self.last_exchange_us:
            self.last_exchange_us = trade.exchange_ts_us
        return True, records

    def _check_skew(self, trade: Trade) -> list[GapRecord]:
        latency = trade.latency_us
        if abs(latency) > self.max_skew_us:
            if self._skew is None:
                self._skew = _Skew(trade.exchange_ts_us, trade.exchange_ts_us, latency, 1)
            else:
                self._skew.end_us = trade.exchange_ts_us
                self._skew.count += 1
                if abs(latency) > abs(self._skew.worst_us):
                    self._skew.worst_us = latency
            return []
        if self._skew is not None:
            return [self._end_skew(trade.recv_ts_us)]
        return []

    def _end_skew(self, now_us: int) -> GapRecord:
        assert self._skew is not None
        skew = self._skew
        self._skew = None
        return self._record(
            "clock_skew",
            skew.start_us,
            skew.end_us,
            now_us,
            detail=(
                f"{skew.count} trade(s) with |receive - exchange time| above "
                f"{self.max_skew_us / 1e6:g} s; worst {skew.worst_us / 1e6:+.3f} s"
            ),
        )

    def close(self, now_us: int) -> list[GapRecord]:
        """Flush open windows at shutdown."""
        records: list[GapRecord] = []
        if self._disconnect is not None:
            info = self._disconnect
            self._disconnect = None
            start = self.last_exchange_us if self.last_exchange_us is not None else info.since_us
            records.append(
                self._record(
                    "disconnect",
                    start,
                    now_us,
                    now_us,
                    detail=f"connection lost: {info.reason}; recording stopped before reconnect",
                )
            )
        if self._skew is not None:
            records.append(self._end_skew(now_us))
        return records
