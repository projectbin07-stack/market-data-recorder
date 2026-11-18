"""Integer-microsecond timestamps.

All timestamps inside the recorder are integer microseconds since the Unix epoch (UTC).
Integers keep ordering and arithmetic exact; text is produced only when writing files.
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timedelta, timezone

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
US_PER_S = 1_000_000

_ISO = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[Tt ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?"
    r"(Z|z|[+-]\d{2}:?\d{2})?$"
)
_INTERVAL = re.compile(r"^(\d+)([smhd])$")
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_SIZE = re.compile(r"^(\d+)\s*(B|KB|MB|GB|KiB|MiB|GiB)?$", re.IGNORECASE)
_SIZE_UNITS = {
    "b": 1,
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
}


def now_us() -> int:
    """Current wall-clock time in integer microseconds."""
    return time.time_ns() // 1000


def iso_to_us(text: str) -> int:
    """Parse an RFC 3339 / ISO 8601 timestamp with up to nine fractional digits.

    Naive timestamps are taken as UTC. Fractions finer than a microsecond are truncated.
    The standard library parser on Python 3.10 accepts only 3 or 6 fractional digits,
    which is why this is done by hand.
    """
    match = _ISO.match(text.strip())
    if match is None:
        raise ValueError(f"not an ISO 8601 timestamp: {text!r}")
    year, month, day, hour, minute, second, frac, tz = match.groups()
    dt = datetime(
        int(year), int(month), int(day), int(hour), int(minute), int(second), tzinfo=timezone.utc
    )
    if tz and tz not in ("Z", "z"):
        sign = 1 if tz[0] == "+" else -1
        digits = tz[1:].replace(":", "")
        dt -= sign * timedelta(hours=int(digits[:2]), minutes=int(digits[2:]))
    micros = int((frac or "").ljust(6, "0")[:6])
    return (dt - EPOCH) // timedelta(microseconds=1) + micros


def us_to_iso(us: int) -> str:
    """Microsecond-precision UTC text, e.g. ``2026-01-02T03:04:05.123456Z``."""
    return (EPOCH + timedelta(microseconds=us)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def us_to_iso_seconds(us: int) -> str:
    """Second-precision UTC text, e.g. ``2026-01-02T03:04:05Z`` (fraction dropped)."""
    return (EPOCH + timedelta(microseconds=us)).strftime("%Y-%m-%dT%H:%M:%SZ")


def us_to_compact(us: int) -> str:
    """Filename-safe UTC text, e.g. ``20260102T030405Z``."""
    return (EPOCH + timedelta(microseconds=us)).strftime("%Y%m%dT%H%M%SZ")


def parse_interval(text: str) -> int:
    """Parse ``30s``, ``1m``, ``4h`` or ``1d`` into whole seconds."""
    match = _INTERVAL.match(text.strip())
    if match is None:
        raise ValueError(f"bad interval {text!r}; use e.g. 30s, 1m, 15m, 1h, 1d")
    seconds = int(match.group(1)) * _UNITS[match.group(2)]
    if seconds <= 0:
        raise ValueError("interval must be positive")
    return seconds


def format_interval(seconds: int) -> str:
    """Inverse of :func:`parse_interval`, choosing the largest exact unit."""
    for unit in ("d", "h", "m"):
        if seconds % _UNITS[unit] == 0:
            return f"{seconds // _UNITS[unit]}{unit}"
    return f"{seconds}s"


def parse_size(text: str) -> int:
    """Parse ``65536``, ``512KB`` or ``64MiB`` into bytes."""
    match = _SIZE.match(text.strip())
    if match is None:
        raise ValueError(f"bad size {text!r}; use e.g. 512KB, 64MB, 1GiB")
    value = int(match.group(1)) * _SIZE_UNITS[(match.group(2) or "b").lower()]
    if value <= 0:
        raise ValueError("size must be positive")
    return value
