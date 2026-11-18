"""CSV layouts for trades and bars, compatible with tickreplay's readers.

tickreplay's documented input rules (``tickreplay/data/csvio.py``), which every file
written here satisfies without conversion:

- lines starting with ``#`` are comments; the first other line is the header;
- trade files need ``timestamp,price,size`` (``symbol`` optional), bar files need
  ``timestamp,open,high,low,close,volume`` (``symbol`` optional); extra columns are
  allowed, duplicate column names are not, and every row has as many fields as the header;
- timestamps are ISO 8601 (``Z`` accepted) or integer epoch seconds;
- trade prices are positive and sizes non-negative; trade timestamps are non-decreasing
  through the whole file (the recorder writes one symbol per file);
- bar prices are positive, ``low <= open, close <= high``, volume non-negative, and bar
  timestamps (the bar's *open* time) strictly increase per symbol;
- a file must contain at least one data row.

:func:`check_tickreplay_trades` and :func:`check_tickreplay_bars` re-implement those rules
so ``mdrec verify`` can check files without tickreplay installed.
"""

from __future__ import annotations

import csv
import io
from collections.abc import Iterable, Iterator, Sequence
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from market_data_recorder.models import Bar, TakerSide, Trade
from market_data_recorder.timeutil import iso_to_us, us_to_iso, us_to_iso_seconds

TRADE_COLUMNS = (
    "timestamp",
    "symbol",
    "price",
    "size",
    "side",
    "trade_id",
    "recv_timestamp",
    "backfilled",
)
BAR_COLUMNS = (
    "timestamp",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trades",
    "partial",
    "first_trade_id",
    "last_trade_id",
)


def _num(value: Decimal) -> str:
    """Plain positional notation (never ``1E-8``), preserving the source's digits."""
    return format(value, "f")


def _opt_int(value: int | None) -> str:
    return "" if value is None else str(value)


def trade_row(trade: Trade) -> str:
    """One CSV line (with trailing newline) for a trade."""
    fields = [
        us_to_iso(trade.exchange_ts_us),
        trade.symbol,
        _num(trade.price),
        _num(trade.size),
        trade.side,
        _opt_int(trade.trade_id),
        us_to_iso(trade.recv_ts_us),
        "1" if trade.backfilled else "0",
    ]
    return _line(fields)


def bar_row(bar: Bar) -> str:
    """One CSV line for a bar; the timestamp is the bar's open time."""
    fields = [
        us_to_iso_seconds(bar.open_ts_us),
        bar.symbol,
        _num(bar.open),
        _num(bar.high),
        _num(bar.low),
        _num(bar.close),
        _num(bar.volume),
        str(bar.trades),
        "1" if bar.partial else "0",
        _opt_int(bar.first_trade_id),
        _opt_int(bar.last_trade_id),
    ]
    return _line(fields)


def header_row(columns: Sequence[str]) -> str:
    return _line(list(columns))


def _line(fields: Sequence[str]) -> str:
    buffer = io.StringIO()
    csv.writer(buffer, lineterminator="\n").writerow(fields)
    return buffer.getvalue()


def iter_rows(path: Path) -> Iterator[tuple[int, dict[str, str]]]:
    """Yield ``(line_number, row)`` for the data rows of a recorder CSV."""
    header: list[str] | None = None
    with path.open(encoding="utf-8", newline="") as handle:
        for number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            values = next(csv.reader([line]))
            if header is None:
                header = [v.strip().lower() for v in values]
                continue
            if len(values) != len(header):
                raise ValueError(
                    f"line {number}: expected {len(header)} fields, found {len(values)}"
                )
            yield number, dict(zip(header, values, strict=True))


def read_comments(path: Path) -> list[str]:
    """The ``#`` comment lines before the header, without the leading ``#``."""
    comments: list[str] = []
    with path.open(encoding="utf-8", newline="") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            if not stripped.startswith("#"):
                break
            comments.append(stripped[1:].strip())
    return comments


def parse_trade(row: dict[str, str], source: str) -> Trade:
    trade_id = row.get("trade_id", "")
    return Trade(
        source=source,
        symbol=row["symbol"],
        trade_id=int(trade_id) if trade_id else None,
        price=Decimal(row["price"]),
        size=Decimal(row["size"]),
        side=_side(row.get("side", "")),
        exchange_ts_us=iso_to_us(row["timestamp"]),
        recv_ts_us=iso_to_us(row["recv_timestamp"]) if row.get("recv_timestamp") else 0,
        backfilled=row.get("backfilled") == "1",
    )


def _side(text: str) -> TakerSide:
    if text == "buy":
        return "buy"
    if text == "sell":
        return "sell"
    return ""


def read_trades(path: Path, source: str) -> list[Trade]:
    return [parse_trade(row, source) for _, row in iter_rows(path)]


# -- tickreplay rule checks -----------------------------------------------------------


def _tr_timestamp(text: str) -> datetime:
    """tickreplay's timestamp parser: ISO 8601 or integer epoch seconds."""
    raw = text.strip()
    if not raw:
        raise ValueError("empty timestamp")
    if raw.lstrip("-").isdigit():
        return datetime.fromtimestamp(int(raw), tz=timezone.utc)
    if raw[-1] in "zZ":
        raw = raw[:-1] + "+00:00"
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _tr_decimal(row: dict[str, str], column: str) -> Decimal:
    text = (row.get(column) or "").strip()
    try:
        value = Decimal(text)
    except InvalidOperation:
        raise ValueError(f"{column} is not a number: {text!r}") from None
    if not value.is_finite():
        raise ValueError(f"{column} is not finite: {text!r}")
    return value


def _tr_rows(path: Path, required: Iterable[str]) -> tuple[list[tuple[int, dict[str, str]]], bool]:
    data_lines: list[tuple[int, str]] = []
    with path.open(encoding="utf-8-sig", newline="") as handle:
        for number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            data_lines.append((number, line))
    if not data_lines:
        raise ValueError("no header row found")
    header_line, header_text = data_lines[0]
    header = [h.strip().lower() for h in next(csv.reader([header_text]))]
    missing = [c for c in required if c not in header]
    if missing:
        raise ValueError(f"line {header_line}: missing column(s): {', '.join(missing)}")
    if len(set(header)) != len(header):
        raise ValueError(f"line {header_line}: duplicate column names in header")
    rows: list[tuple[int, dict[str, str]]] = []
    for number, text in data_lines[1:]:
        values = next(csv.reader([text]))
        if len(values) != len(header):
            raise ValueError(f"line {number}: expected {len(header)} fields, found {len(values)}")
        rows.append((number, dict(zip(header, values, strict=True))))
    if not rows:
        raise ValueError("file contains a header but no data rows")
    return rows, "symbol" in header


def check_tickreplay_trades(path: Path) -> list[str]:
    """Problems tickreplay's ``read_trades`` would raise for this file (empty = readable)."""
    try:
        rows, has_symbol = _tr_rows(path, ("timestamp", "price", "size"))
    except ValueError as exc:
        return [str(exc)]
    problems: list[str] = []
    last: datetime | None = None
    for line, row in rows:
        try:
            ts = _tr_timestamp(row["timestamp"])
            if last is not None and ts < last:
                raise ValueError("out-of-order timestamp (timestamps must be non-decreasing)")
            last = ts
            if has_symbol and not row["symbol"].strip():
                raise ValueError("empty symbol")
            if _tr_decimal(row, "price") <= 0:
                raise ValueError("trade price must be positive")
            if _tr_decimal(row, "size") < 0:
                raise ValueError("trade size must be non-negative")
        except (ValueError, OverflowError, OSError) as exc:
            problems.append(f"line {line}: {exc}")
    return problems


def check_tickreplay_bars(path: Path) -> list[str]:
    """Problems tickreplay's ``read_bars`` would raise for this file (empty = readable)."""
    try:
        rows, has_symbol = _tr_rows(path, ("timestamp", "open", "high", "low", "close", "volume"))
    except ValueError as exc:
        return [str(exc)]
    problems: list[str] = []
    last_ts: dict[str, datetime] = {}
    for line, row in rows:
        try:
            ts = _tr_timestamp(row["timestamp"])
            symbol = row["symbol"].strip() if has_symbol else "ASSET"
            if not symbol:
                raise ValueError("empty symbol")
            previous = last_ts.get(symbol)
            if previous is not None and ts <= previous:
                raise ValueError(f"timestamps must be strictly increasing for {symbol}")
            last_ts[symbol] = ts
            o, h, lo, c, v = (
                _tr_decimal(row, k) for k in ("open", "high", "low", "close", "volume")
            )
            if min(o, h, lo, c) <= 0:
                raise ValueError("bar prices must be positive")
            if h < max(o, c, lo):
                raise ValueError("bar high is below open/close/low")
            if lo > min(o, c, h):
                raise ValueError("bar low is above open/close/high")
            if v < 0:
                raise ValueError("bar volume must be non-negative")
        except (ValueError, OverflowError, OSError) as exc:
            problems.append(f"line {line}: {exc}")
    return problems
