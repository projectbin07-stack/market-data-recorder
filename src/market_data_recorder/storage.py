"""Append-only, rotated, checksummed segment files and the manifest that lists them.

A segment is written to ``<name>.part`` and only becomes ``<name>`` (via ``os.replace``)
once it is complete and fsynced; at that moment its SHA-256, size, row count and time
range are added to ``manifest.json``, which is itself replaced atomically. A crash can
therefore leave at most one ``.part`` file per stream, never a half-written final segment
or a corrupt manifest. ``mdrec verify --salvage`` turns leftover ``.part`` files into
segments marked ``recovered``, keeping only whole, parseable lines.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import sys
import time
from collections.abc import Callable, Iterator
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, BinaryIO, Literal

from market_data_recorder import __version__
from market_data_recorder.csvformat import iter_rows
from market_data_recorder.timeutil import iso_to_us, now_us, us_to_compact

Kind = Literal["trades", "bars", "gaps"]
MANIFEST = "manifest.json"
MANIFEST_FORMAT = "market-data-recorder/1"
HEADER_TAG = "market-data-recorder"
DISCLAIMER = "Recorded public market data. Not investment information or advice."


def symbol_dir(symbol: str) -> str:
    """Filesystem-safe directory name for a symbol (``BTC/USD`` -> ``BTC_USD``)."""
    return re.sub(r"[^A-Za-z0-9._-]", "_", symbol) or "_"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def replace_with_retry(src: Path, dst: Path, attempts: int = 20) -> None:
    """``os.replace``, retrying briefly when Windows reports the target as in use."""
    for attempt in range(attempts):
        try:
            os.replace(src, dst)
            return
        except PermissionError:
            if attempt == attempts - 1:
                raise
            time.sleep(0.05)


def fsync_dir(path: Path) -> None:
    """Persist a rename on POSIX; directories cannot be opened for fsync on Windows."""
    if sys.platform == "win32":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@dataclass
class SegmentEntry:
    """One finished segment as recorded in the manifest."""

    path: str
    kind: Kind
    source: str
    symbol: str | None
    session: str
    rows: int
    bytes: int
    sha256: str
    first_ts_us: int | None
    last_ts_us: int | None
    first_trade_id: int | None = None
    last_trade_id: int | None = None
    interval_s: int | None = None
    backfill: bool = False
    recovered: bool = False
    closed_us: int = 0

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SegmentEntry:
        known = set(cls.__dataclass_fields__)
        return cls(**{k: v for k, v in data.items() if k in known})


@dataclass
class Manifest:
    """``manifest.json``: the authoritative list of finished segments under ``root``."""

    root: Path
    segments: list[SegmentEntry] = field(default_factory=list)

    @classmethod
    def load(cls, root: Path) -> Manifest:
        path = root / MANIFEST
        if not path.exists():
            return cls(root)
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("format") != MANIFEST_FORMAT:
            raise ValueError(f"{path}: unknown manifest format {data.get('format')!r}")
        return cls(root, [SegmentEntry.from_dict(s) for s in data.get("segments", [])])

    def add(self, entry: SegmentEntry) -> None:
        self.segments.append(entry)
        self.save()

    def save(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        body = {
            "format": MANIFEST_FORMAT,
            "writer": f"market-data-recorder {__version__}",
            "note": DISCLAIMER,
            "segments": [asdict(s) for s in self.segments],
        }
        tmp = self.root / (MANIFEST + ".tmp")
        with tmp.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(body, handle, indent=1)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        replace_with_retry(tmp, self.root / MANIFEST)
        fsync_dir(self.root)

    def tracked(self) -> set[str]:
        return {s.path for s in self.segments}


def csv_header(kind: Kind, source: str, symbol: str | None, session: str, extra: str = "") -> str:
    """Comment lines that start every CSV segment (tickreplay keeps them as data notes)."""
    tag = f"# {HEADER_TAG} kind={kind} source={source} symbol={symbol or ''} session={session}"
    if extra:
        tag += f" {extra}"
    return f"{tag}\n# {DISCLAIMER}\n"


def parse_header_tag(line: str) -> dict[str, str]:
    """Read the ``key=value`` pairs from a segment's first comment line."""
    text = line.lstrip("#").strip()
    if not text.startswith(HEADER_TAG):
        return {}
    pairs = (p.split("=", 1) for p in text[len(HEADER_TAG) :].split() if "=" in p)
    return dict(pairs)


@dataclass
class SegmentStats:
    rows: int = 0
    first_ts_us: int | None = None
    last_ts_us: int | None = None
    first_trade_id: int | None = None
    last_trade_id: int | None = None

    def add(self, ts_us: int | None, first_id: int | None, last_id: int | None) -> None:
        self.rows += 1
        if ts_us is not None:
            if self.first_ts_us is None:
                self.first_ts_us = ts_us
            self.last_ts_us = ts_us
        if first_id is not None and self.first_trade_id is None:
            self.first_trade_id = first_id
        if last_id is not None:
            self.last_trade_id = last_id


def _opt_int(text: str | None) -> int | None:
    return int(text) if text else None


def scan_rows(path: Path, kind: Kind) -> Iterator[tuple[int | None, int | None, int | None]]:
    """Yield ``(timestamp_us, first_id, last_id)`` per data row, validating as it goes."""
    if kind == "gaps":
        with path.open(encoding="utf-8") as handle:
            for number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except ValueError:
                    raise ValueError(f"line {number}: not valid JSON") from None
                if record.get("type") == "header":
                    continue
                yield int(record.get("detected_us", 0)), None, None
        return
    for _, row in iter_rows(path):
        ts = iso_to_us(row["timestamp"])
        if kind == "trades":
            tid = _opt_int(row.get("trade_id"))
            yield ts, tid, tid
        else:
            yield ts, _opt_int(row.get("first_trade_id")), _opt_int(row.get("last_trade_id"))


def scan_segment(path: Path, kind: Kind) -> SegmentStats:
    stats = SegmentStats()
    for ts, first, last in scan_rows(path, kind):
        stats.add(ts, first, last)
    return stats


class SegmentWriter:
    """Write one stream (one kind, source and symbol) as a sequence of rotated segments."""

    def __init__(
        self,
        root: Path,
        manifest: Manifest,
        *,
        kind: Kind,
        source: str,
        symbol: str | None,
        directory: Path,
        prefix: str,
        header: str,
        session: str,
        rotate_bytes: int,
        rotate_seconds: float,
        interval_s: int | None = None,
        backfill: bool = False,
        strict_ts: bool = False,
        clock: Callable[[], int] = now_us,
    ) -> None:
        self.root = root
        self.manifest = manifest
        self.kind: Kind = kind
        self.source = source
        self.symbol = symbol
        self.directory = directory
        self.prefix = prefix
        self.header = header
        self.session = session
        self.rotate_bytes = rotate_bytes
        self.rotate_us = int(rotate_seconds * 1_000_000)
        self.interval_s = interval_s
        self.backfill = backfill
        self.strict_ts = strict_ts
        self.clock = clock
        self.finished: list[SegmentEntry] = []
        self._seq = 0
        self._handle: BinaryIO | None = None
        self._final: Path | None = None
        self._part: Path | None = None
        self._opened_us = 0
        self._sha = hashlib.sha256()
        self._bytes = 0
        self._stats = SegmentStats()

    @property
    def part_path(self) -> Path | None:
        return self._part

    def _open(self) -> None:
        self._seq += 1
        self._opened_us = self.clock()
        folder = self.root / self.directory
        folder.mkdir(parents=True, exist_ok=True)
        ext = "jsonl" if self.kind == "gaps" else "csv"
        name = (
            f"{self.prefix}-{us_to_compact(self._opened_us)}-{self.session}-{self._seq:04d}.{ext}"
        )
        self._final = folder / name
        self._part = folder / (name + ".part")
        self._handle = self._part.open("xb")
        self._sha = hashlib.sha256()
        self._bytes = 0
        self._stats = SegmentStats()
        self._raw(self.header.encode("utf-8"))

    def _raw(self, data: bytes) -> None:
        assert self._handle is not None
        self._handle.write(data)
        self._sha.update(data)
        self._bytes += len(data)

    def write(
        self,
        line: str,
        ts_us: int | None,
        first_id: int | None = None,
        last_id: int | None = None,
    ) -> None:
        """Append one row. Rotates first if the row would break timestamp order."""
        last = self._stats.last_ts_us
        if self._handle is not None and ts_us is not None and last is not None:
            regress = ts_us <= last if self.strict_ts else ts_us < last
            if regress:
                self.finalize()
        if self._handle is None:
            self._open()
        self._raw(line.encode("utf-8"))
        self._stats.add(ts_us, first_id, last_id)
        if self._bytes >= self.rotate_bytes:
            self.finalize()

    def flush(self) -> None:
        """Push buffered rows to disk (fsync) so a crash loses at most this interval."""
        if self._handle is not None:
            self._handle.flush()
            os.fsync(self._handle.fileno())

    def maybe_rotate(self, now: int) -> None:
        if self._handle is not None and now - self._opened_us >= self.rotate_us:
            self.finalize()

    def finalize(self) -> SegmentEntry | None:
        """Close the open segment, rename it into place and record it in the manifest."""
        if self._handle is None:
            return None
        assert self._part is not None and self._final is not None
        handle, self._handle = self._handle, None
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        if self._stats.rows == 0:
            self._part.unlink()
            return None
        replace_with_retry(self._part, self._final)
        fsync_dir(self._final.parent)
        entry = SegmentEntry(
            path=self._final.relative_to(self.root).as_posix(),
            kind=self.kind,
            source=self.source,
            symbol=self.symbol,
            session=self.session,
            rows=self._stats.rows,
            bytes=self._bytes,
            sha256=self._sha.hexdigest(),
            first_ts_us=self._stats.first_ts_us,
            last_ts_us=self._stats.last_ts_us,
            first_trade_id=self._stats.first_trade_id,
            last_trade_id=self._stats.last_trade_id,
            interval_s=self.interval_s,
            backfill=self.backfill,
            closed_us=self.clock(),
        )
        self.manifest.add(entry)
        self.finished.append(entry)
        return entry


def find_parts(root: Path) -> list[Path]:
    """Unfinished segments (``*.part``) under ``root``."""
    return sorted(p for p in root.rglob("*.part") if p.is_file())


def _header_info(lines: list[bytes]) -> dict[str, str]:
    if not lines:
        return {}
    first = lines[0].decode("utf-8", errors="replace")
    if first.startswith("#"):
        return parse_header_tag(first)
    try:
        data = json.loads(first)
    except ValueError:
        return {}
    if isinstance(data, dict) and data.get("type") == "header":
        return {str(k): str(v) for k, v in data.items()}
    return {}


def _check_row(kind: Kind, row: dict[str, str]) -> None:
    """Raise if a CSV data row is not complete and well-formed."""
    iso_to_us(row["timestamp"])
    columns = ("price", "size") if kind == "trades" else ("open", "high", "low", "close", "volume")
    for column in columns:
        if not Decimal(row[column]).is_finite():
            raise ValueError(f"{column} is not finite")


def salvage_part(root: Path, part: Path, manifest: Manifest) -> SegmentEntry | None:
    """Turn a crash-orphaned ``.part`` file into a finished segment marked ``recovered``.

    Only complete lines are kept, and data rows are kept up to the first one that does
    not parse. Returns ``None`` (and removes the file) if no data row survives.
    """
    content = part.read_bytes()
    lines = content.split(b"\n")[:-1]  # drop the unterminated tail, if any
    info = _header_info(lines)
    kind_text = info.get("kind")
    if kind_text not in ("trades", "bars", "gaps"):
        raise ValueError(f"{part}: not a market-data-recorder segment (no header tag)")
    kind: Kind = kind_text  # type: ignore[assignment]
    final = part.with_name(part.name[: -len(".part")])
    tmp = part.with_name(part.name + ".salvage")
    kept: list[bytes] = []
    header: list[str] | None = None
    for line in lines:
        try:
            text = line.decode("utf-8")
            if kind == "gaps":
                json.loads(text)
            elif text.strip() and not text.strip().startswith("#"):
                values = next(csv.reader([text]))
                if header is None:
                    header = [v.strip().lower() for v in values]
                else:
                    _check_row(kind, dict(zip(header, values, strict=True)))
        except (ValueError, KeyError, ArithmeticError):
            break  # strict=True zip raises ValueError on a short row
        kept.append(line)
    data = b"\n".join(kept) + b"\n" if kept else b""
    with tmp.open("wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    stats = scan_segment(tmp, kind)
    if stats.rows == 0:
        tmp.unlink()
        part.unlink()
        return None
    replace_with_retry(tmp, final)
    part.unlink()
    fsync_dir(final.parent)
    interval = info.get("interval_s")
    entry = SegmentEntry(
        path=final.relative_to(root).as_posix(),
        kind=kind,
        source=info.get("source", ""),
        symbol=info.get("symbol") or None,
        session=info.get("session", ""),
        rows=stats.rows,
        bytes=len(data),
        sha256=hashlib.sha256(data).hexdigest(),
        first_ts_us=stats.first_ts_us,
        last_ts_us=stats.last_ts_us,
        first_trade_id=stats.first_trade_id,
        last_trade_id=stats.last_trade_id,
        interval_s=int(interval) if interval else None,
        backfill=info.get("backfill") == "1",
        recovered=True,
        closed_us=now_us(),
    )
    manifest.add(entry)
    return entry
