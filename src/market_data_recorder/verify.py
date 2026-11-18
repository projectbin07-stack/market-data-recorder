"""``mdrec verify``: prove a recording directory is intact and say exactly what is missing."""

from __future__ import annotations

import itertools
import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from market_data_recorder.csvformat import (
    check_tickreplay_bars,
    check_tickreplay_trades,
    iter_rows,
)
from market_data_recorder.models import GapRecord
from market_data_recorder.storage import (
    MANIFEST,
    Manifest,
    SegmentEntry,
    find_parts,
    salvage_part,
    scan_segment,
    sha256_file,
)
from market_data_recorder.timeutil import us_to_iso

#: Output of ``mdrec bars`` lives here and is deliberately not part of the manifest.
REBUILT_DIR = "rebuilt"


@dataclass(frozen=True)
class Finding:
    level: Literal["error", "warning", "info"]
    path: str
    message: str


@dataclass
class GapSummary:
    count: Counter[str] = field(default_factory=Counter)
    missing: Counter[str] = field(default_factory=Counter)
    unknown_missing: Counter[str] = field(default_factory=Counter)
    duration_us: Counter[str] = field(default_factory=Counter)
    backfill_requested: int = 0
    backfill_received: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "count": dict(self.count),
            "missing_trades": dict(self.missing),
            "unknown_missing": dict(self.unknown_missing),
            "duration_s": {k: v / 1e6 for k, v in self.duration_us.items()},
            "backfill_requested": self.backfill_requested,
            "backfill_received": self.backfill_received,
        }


@dataclass
class VerifyReport:
    root: Path
    segments: int = 0
    rows: int = 0
    findings: list[Finding] = field(default_factory=list)
    gaps: list[GapRecord] = field(default_factory=list)
    backfills: list[dict[str, Any]] = field(default_factory=list)
    salvaged: list[str] = field(default_factory=list)

    @property
    def errors(self) -> list[Finding]:
        return [f for f in self.findings if f.level == "error"]

    @property
    def ok(self) -> bool:
        return not self.errors

    def error(self, path: str, message: str) -> None:
        self.findings.append(Finding("error", path, message))

    def warn(self, path: str, message: str) -> None:
        self.findings.append(Finding("warning", path, message))

    def info(self, path: str, message: str) -> None:
        self.findings.append(Finding("info", path, message))

    def gap_summary(self) -> GapSummary:
        return summarize_gaps(self.gaps, self.backfills)

    def as_dict(self) -> dict[str, Any]:
        return {
            "root": str(self.root),
            "ok": self.ok,
            "segments": self.segments,
            "rows": self.rows,
            "salvaged": self.salvaged,
            "findings": [f.__dict__ for f in self.findings],
            "gaps": self.gap_summary().as_dict(),
        }


def summarize_gaps(gaps: list[GapRecord], backfills: list[dict[str, Any]]) -> GapSummary:
    summary = GapSummary()
    for gap in gaps:
        summary.count[gap.reason] += 1
        summary.duration_us[gap.reason] += gap.end_us - gap.start_us
        if gap.missing is None:
            if gap.reason in ("disconnect", "sequence_jump"):
                summary.unknown_missing[gap.reason] += 1
        else:
            summary.missing[gap.reason] += gap.missing
    for record in backfills:
        summary.backfill_requested += int(record.get("requested", 0))
        summary.backfill_received += int(record.get("received", 0))
    return summary


def load_gap_files(
    root: Path, entries: list[SegmentEntry]
) -> tuple[list[GapRecord], list[dict[str, Any]]]:
    """Gap and backfill records from every gap segment listed in the manifest."""
    gaps: list[GapRecord] = []
    backfills: list[dict[str, Any]] = []
    for entry in entries:
        if entry.kind != "gaps":
            continue
        path = root / entry.path
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            if record.get("type") == "gap":
                gaps.append(GapRecord.from_dict(record))
            elif record.get("type") == "backfill":
                backfills.append(record)
    return gaps, backfills


def _check_entry(root: Path, entry: SegmentEntry, report: VerifyReport) -> list[int] | None:
    """Check one segment; return its trade ids (live trade segments only)."""
    path = root / entry.path
    if not path.exists():
        report.error(entry.path, "listed in manifest but missing on disk")
        return None
    size = path.stat().st_size
    if size != entry.bytes:
        report.error(entry.path, f"size {size} != manifest {entry.bytes}")
        return None
    digest = sha256_file(path)
    if digest != entry.sha256:
        report.error(entry.path, "SHA-256 does not match the manifest (file changed or corrupt)")
        return None
    try:
        stats = scan_segment(path, entry.kind)
    except (ValueError, KeyError, ArithmeticError) as exc:
        report.error(entry.path, f"unreadable: {exc}")
        return None
    if stats.rows != entry.rows:
        report.error(entry.path, f"{stats.rows} rows, manifest says {entry.rows}")
    if (stats.first_ts_us, stats.last_ts_us) != (entry.first_ts_us, entry.last_ts_us):
        report.error(entry.path, "time range differs from the manifest")
    report.rows += stats.rows
    if entry.kind == "bars":
        for problem in check_tickreplay_bars(path):
            report.error(entry.path, f"tickreplay bar rule: {problem}")
        return None
    if entry.kind != "trades":
        return None
    for problem in check_tickreplay_trades(path):
        report.error(entry.path, f"tickreplay trade rule: {problem}")
    ids: list[int] = []
    for line, row in iter_rows(path):
        if not entry.backfill and not row.get("recv_timestamp"):
            report.error(entry.path, f"line {line}: live trade without recv_timestamp")
        if row.get("trade_id"):
            tid = int(row["trade_id"])
            if ids and tid <= ids[-1]:
                report.error(entry.path, f"line {line}: trade id {tid} not above {ids[-1]}")
            ids.append(tid)
    return None if entry.backfill else ids


def _covered(gaps: list[GapRecord], first: int, last: int) -> bool:
    """True if the union of the gap records' missing-id ranges covers ``[first, last]``.

    One hole can be reported by several records (for example a disconnect followed by a
    heartbeat-detected jump), so ranges are merged before checking.
    """
    ranges = sorted(
        (g.first_missing_id, g.last_missing_id)
        for g in gaps
        if g.first_missing_id is not None and g.last_missing_id is not None
    )
    need = first
    for lo, hi in ranges:
        if lo > need:
            break
        need = max(need, hi + 1)
    return need > last


def verify(root: Path, *, salvage: bool = False) -> VerifyReport:
    """Check checksums, row counts, tickreplay rules, ordering and gap accounting."""
    report = VerifyReport(root)
    if not (root / MANIFEST).exists():
        report.error(MANIFEST, "no manifest.json - not a recording directory")
        return report
    manifest = Manifest.load(root)

    for part in find_parts(root):
        rel = part.relative_to(root).as_posix()
        if salvage:
            try:
                entry = salvage_part(root, part, manifest)
            except (ValueError, OSError) as exc:
                report.error(rel, f"could not salvage: {exc}")
                continue
            if entry is None:
                report.info(rel, "unfinished segment had no complete rows; removed")
            else:
                report.salvaged.append(entry.path)
                report.info(entry.path, f"salvaged {entry.rows} complete row(s) from {rel}")
        else:
            report.warn(
                rel,
                "unfinished segment (recorder still running, or it crashed); "
                "not covered by the manifest - `mdrec verify --salvage` recovers whole rows",
            )

    tracked = manifest.tracked()
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix not in (".csv", ".jsonl"):
            continue
        rel = path.relative_to(root).as_posix()
        if rel.split("/")[0] == REBUILT_DIR or rel in tracked:
            continue
        report.warn(rel, "file is not listed in the manifest")

    report.gaps, report.backfills = load_gap_files(root, manifest.segments)
    gaps_by_stream: dict[tuple[str, str], list[GapRecord]] = defaultdict(list)
    for gap in report.gaps:
        gaps_by_stream[(gap.source, gap.symbol)].append(gap)

    live: dict[tuple[str, str], list[tuple[SegmentEntry, list[int]]]] = defaultdict(list)
    for entry in manifest.segments:
        report.segments += 1
        ids = _check_entry(root, entry, report)
        if ids is not None and entry.symbol is not None:
            live[(entry.source, entry.symbol)].append((entry, ids))

    for key, segments in live.items():
        segments.sort(key=lambda item: (item[0].first_ts_us or 0, item[0].path))
        stream_gaps = gaps_by_stream.get(key, [])
        previous: tuple[SegmentEntry, int] | None = None
        for entry, ids in segments:
            for a, b in itertools.pairwise(ids):
                if b > a + 1 and not _covered(stream_gaps, a + 1, b - 1):
                    report.error(
                        entry.path, f"trade ids jump {a} -> {b} with no gap record covering it"
                    )
            if ids and previous is not None:
                prev_entry, prev_last = previous
                first = ids[0]
                if first > prev_last + 1 and not _covered(stream_gaps, prev_last + 1, first - 1):
                    if prev_entry.session == entry.session:
                        report.error(
                            entry.path,
                            f"trade ids jump {prev_last} -> {first} across segments "
                            "with no gap record covering it",
                        )
                    else:
                        report.info(
                            entry.path,
                            f"not recording between sessions: trade ids {prev_last + 1}"
                            f"..{first - 1} were not captured",
                        )
                elif first <= prev_last:
                    report.warn(
                        entry.path, f"overlaps earlier segment (first id {first} <= {prev_last})"
                    )
            if ids:
                previous = (entry, ids[-1])
    return report


def format_report(report: VerifyReport) -> str:
    lines = [
        f"verified {report.segments} segment(s), {report.rows} row(s) under {report.root}",
    ]
    for finding in report.findings:
        lines.append(f"  {finding.level.upper():7} {finding.path}: {finding.message}")
    summary = report.gap_summary()
    if summary.count:
        lines.append("gap report:")
        for reason in sorted(summary.count):
            known = summary.missing.get(reason, 0)
            unknown = summary.unknown_missing.get(reason, 0)
            extra = f", {unknown} with unknown loss" if unknown else ""
            lines.append(
                f"  {reason:14} {summary.count[reason]:5} record(s), "
                f"{summary.duration_us[reason] / 1e6:10.3f} s, "
                f"{known} trade(s) known missing{extra}"
            )
        if summary.backfill_requested:
            lines.append(
                f"  backfill: {summary.backfill_received} of {summary.backfill_requested} "
                "missing trade(s) recovered from REST"
            )
        for gap in report.gaps:
            if gap.loses_data:
                lines.append(
                    f"    {gap.source} {gap.symbol} {gap.reason} {us_to_iso(gap.start_us)} .. "
                    f"{us_to_iso(gap.end_us)} missing={gap.missing}"
                )
    else:
        lines.append("gap report: no gap records")
    errors = len(report.errors)
    lines.append("OK" if report.ok else f"FAILED: {errors} error(s)")
    return "\n".join(lines)
