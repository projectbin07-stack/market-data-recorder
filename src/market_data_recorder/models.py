"""The common data model every source adapter normalises to."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any, Literal

from market_data_recorder.timeutil import us_to_iso

TakerSide = Literal["buy", "sell", ""]
GapReason = Literal["disconnect", "sequence_jump", "clock_skew", "out_of_order"]
NoticeKind = Literal["ack", "error", "info", "pong"]


@dataclass(frozen=True, slots=True)
class Trade:
    """One public trade print.

    ``side`` is the *taker* side (``""`` when the source does not say). ``exchange_ts_us``
    is the exchange's trade time and ``recv_ts_us`` the local wall-clock time at which the
    message carrying it was received. ``backfilled`` trades came from a REST endpoint after
    a gap, not from the live stream.
    """

    source: str
    symbol: str
    trade_id: int | None
    price: Decimal
    size: Decimal
    side: TakerSide
    exchange_ts_us: int
    recv_ts_us: int
    backfilled: bool = False

    def __post_init__(self) -> None:
        if not self.price.is_finite() or self.price <= 0:
            raise ValueError(f"trade price must be positive and finite, got {self.price}")
        if not self.size.is_finite() or self.size < 0:
            raise ValueError(f"trade size must be non-negative and finite, got {self.size}")

    @property
    def latency_us(self) -> int:
        """Local receive time minus exchange time (negative if the clocks disagree)."""
        return self.recv_ts_us - self.exchange_ts_us


@dataclass(frozen=True, slots=True)
class Bar:
    """An OHLCV bar built locally from trades. ``open_ts_us`` is the interval's open time.

    ``partial`` is true when the bar may be missing trades: it started before recording
    began, overlapped a gap that lost data, or was cut short by shutdown.
    """

    source: str
    symbol: str
    interval_s: int
    open_ts_us: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trades: int
    partial: bool
    first_trade_id: int | None
    last_trade_id: int | None

    @property
    def close_ts_us(self) -> int:
        """Exclusive end of the bar's interval."""
        return self.open_ts_us + self.interval_s * 1_000_000


@dataclass(frozen=True, slots=True)
class Heartbeat:
    """A liveness message. ``last_trade_id`` is set when the source reports it."""

    symbol: str | None
    last_trade_id: int | None = None


@dataclass(frozen=True, slots=True)
class Notice:
    """A control message: subscription ack, error, pong or other information."""

    kind: NoticeKind
    text: str


Event = Trade | Heartbeat | Notice


@dataclass(frozen=True, slots=True)
class GapRecord:
    """An explicit record of data that may be missing or suspect.

    ``start_us``/``end_us`` bound the affected window (exchange time where known).
    ``missing`` is the number of trades known to be lost from trade-id continuity, ``0``
    when ids prove nothing was lost, and ``None`` when the source gives no way to tell.
    """

    gap_id: str
    source: str
    symbol: str
    reason: GapReason
    start_us: int
    end_us: int
    detected_us: int
    first_missing_id: int | None = None
    last_missing_id: int | None = None
    missing: int | None = None
    detail: str = ""

    @property
    def loses_data(self) -> bool:
        """True unless trade ids prove that no trade was lost."""
        return self.reason in ("disconnect", "sequence_jump") and self.missing != 0

    def to_json(self) -> str:
        data: dict[str, Any] = {"type": "gap", **asdict(self)}
        data["start"] = us_to_iso(self.start_us)
        data["end"] = us_to_iso(self.end_us)
        return json.dumps(data, sort_keys=True)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> GapRecord:
        return cls(
            gap_id=str(data["gap_id"]),
            source=str(data["source"]),
            symbol=str(data["symbol"]),
            reason=data["reason"],
            start_us=int(data["start_us"]),
            end_us=int(data["end_us"]),
            detected_us=int(data["detected_us"]),
            first_missing_id=data.get("first_missing_id"),
            last_missing_id=data.get("last_missing_id"),
            missing=data.get("missing"),
            detail=str(data.get("detail", "")),
        )
