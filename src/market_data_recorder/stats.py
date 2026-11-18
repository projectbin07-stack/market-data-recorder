"""``mdrec stats``: operational statistics about a recording.

These numbers describe the *recording* - how many trades arrived, how fast, how late
relative to the exchange's timestamps, and what was lost. They are not market analysis
and not investment information.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from market_data_recorder.csvformat import iter_rows
from market_data_recorder.storage import Manifest
from market_data_recorder.timeutil import iso_to_us, us_to_iso
from market_data_recorder.verify import GapSummary, load_gap_files, summarize_gaps

PERCENTILES = (50.0, 90.0, 99.0)


def percentile(sorted_values: list[float], p: float) -> float:
    """Nearest-rank percentile: the smallest value with at least ``p`` % at or below it."""
    if not sorted_values:
        raise ValueError("percentile of an empty list")
    if not 0 < p <= 100:
        raise ValueError("p must be in (0, 100]")
    rank = math.ceil(p / 100 * len(sorted_values))
    return sorted_values[rank - 1]


@dataclass
class StreamStats:
    source: str
    symbol: str
    trades: int = 0
    backfilled: int = 0
    first_recv_us: int | None = None
    last_recv_us: int | None = None
    latencies_ms: list[float] = field(default_factory=list)

    @property
    def span_s(self) -> float:
        if self.first_recv_us is None or self.last_recv_us is None:
            return 0.0
        return (self.last_recv_us - self.first_recv_us) / 1e6

    @property
    def rate(self) -> float | None:
        """Live trades per second of receive time (``None`` if the span is zero)."""
        return self.trades / self.span_s if self.span_s > 0 else None

    def latency(self) -> dict[str, float]:
        values = sorted(self.latencies_ms)
        if not values:
            return {}
        result = {f"p{p:g}": percentile(values, p) for p in PERCENTILES}
        result["min"] = values[0]
        result["max"] = values[-1]
        return result

    @property
    def negative_latency(self) -> int:
        return sum(1 for v in self.latencies_ms if v < 0)

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "symbol": self.symbol,
            "trades": self.trades,
            "backfilled": self.backfilled,
            "first_recv": us_to_iso(self.first_recv_us) if self.first_recv_us else None,
            "last_recv": us_to_iso(self.last_recv_us) if self.last_recv_us else None,
            "span_s": self.span_s,
            "trades_per_s": self.rate,
            "latency_ms": self.latency(),
            "negative_latency_count": self.negative_latency,
        }


@dataclass
class RecordingStats:
    streams: list[StreamStats]
    gaps: GapSummary

    def as_dict(self) -> dict[str, Any]:
        return {
            "streams": [s.as_dict() for s in self.streams],
            "gaps": self.gaps.as_dict(),
            "note": "Operational statistics about the recording; not investment information.",
        }


def compute_stats(root: Path) -> RecordingStats:
    manifest = Manifest.load(root)
    streams: dict[tuple[str, str], StreamStats] = {}
    for entry in manifest.segments:
        if entry.kind != "trades" or entry.symbol is None:
            continue
        key = (entry.source, entry.symbol)
        stream = streams.setdefault(key, StreamStats(entry.source, entry.symbol))
        for _, row in iter_rows(root / entry.path):
            if row.get("backfilled") == "1":
                stream.backfilled += 1
                continue
            recv = iso_to_us(row["recv_timestamp"])
            stream.trades += 1
            stream.latencies_ms.append((recv - iso_to_us(row["timestamp"])) / 1000)
            if stream.first_recv_us is None or recv < stream.first_recv_us:
                stream.first_recv_us = recv
            if stream.last_recv_us is None or recv > stream.last_recv_us:
                stream.last_recv_us = recv
    gaps, backfills = load_gap_files(root, manifest.segments)
    ordered = [streams[k] for k in sorted(streams)]
    return RecordingStats(ordered, summarize_gaps(gaps, backfills))


def format_stats(stats: RecordingStats) -> str:
    lines: list[str] = []
    for s in stats.streams:
        lines.append(f"{s.source} {s.symbol}")
        lines.append(f"  live trades        {s.trades}")
        if s.backfilled:
            lines.append(f"  backfilled trades  {s.backfilled}")
        lines.append(f"  receive span       {s.span_s:.1f} s")
        rate = s.rate
        lines.append(
            f"  trades/s           {rate:.2f}" if rate is not None else "  trades/s           n/a"
        )
        latency = s.latency()
        if latency:
            parts = ", ".join(f"{k} {v:.1f}" for k, v in latency.items())
            lines.append(f"  latency ms         {parts}")
            lines.append(f"  negative latency   {s.negative_latency} trade(s) (clock offset)")
    by_reason: dict[str, int] = defaultdict(int, stats.gaps.count)
    total = sum(by_reason.values())
    lines.append(
        f"gaps: {total} record(s)"
        + "".join(
            f", {reason} {count} ({stats.gaps.missing.get(reason, 0)} trades known missing)"
            for reason, count in sorted(by_reason.items())
        )
    )
    lines.append("latency = local receive time - exchange trade time; includes clock offset.")
    lines.append("Operational statistics about the recording; not investment information.")
    return "\n".join(lines)
