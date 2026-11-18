"""End-to-end: the real ``mdrec record`` process against a local fake exchange.

Covers crash safety (the process is killed mid-write, then verified and salvaged), clean
shutdown on SIGINT/SIGTERM (POSIX only: Windows cannot deliver Ctrl+C to a child process
without a console group), and the ``record`` CLI path in-process.
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from fakeserver import Conn, ThreadedFakeExchange
from market_data_recorder.cli import main
from market_data_recorder.csvformat import check_tickreplay_trades
from market_data_recorder.storage import Manifest, find_parts
from market_data_recorder.timeutil import now_us
from market_data_recorder.verify import verify
from samples import binance_trade


async def stream_forever(conn: Conn, index: int) -> None:
    await conn.recv()
    tid = 1000 * index
    while True:
        tid += 1
        await conn.send(binance_trade(tid, "20000.5", "0.001", now_us()))
        await asyncio.sleep(0.001)


def start_recorder(url: str, out: Path) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "market_data_recorder",
            "record",
            "--source",
            "binance",
            "--symbols",
            "BTCUSDT",
            "--out",
            str(out),
            "--url",
            url,
            "--rotate-size",
            "4KB",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def wait_for(condition: object, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():  # type: ignore[operator]
            return
        time.sleep(0.05)
    raise AssertionError("timed out")


def trade_segments(out: Path) -> int:
    try:
        return sum(1 for s in Manifest.load(out).segments if s.kind == "trades")
    except (ValueError, OSError):
        return 0  # manifest being replaced right now


def part_has_rows(out: Path) -> bool:
    for part in find_parts(out):
        try:
            if "trades-" in part.name and part.stat().st_size > 600:
                return True
        except FileNotFoundError:
            continue  # renamed into place between listing and stat (the recorder is live)
    return False


def test_kill_mid_write_then_verify_and_salvage(tmp_path: Path) -> None:
    out = tmp_path / "data"
    with ThreadedFakeExchange(stream_forever) as exchange:
        proc = start_recorder(exchange.url, out)
        try:
            wait_for(lambda: trade_segments(out) >= 2 and part_has_rows(out))
        finally:
            proc.kill()  # SIGKILL / TerminateProcess: no cleanup code runs
            proc.wait(10)

    finished = Manifest.load(out).segments
    report = verify(out)
    assert report.ok, report.findings  # every finished segment is intact
    assert any("unfinished segment" in f.message for f in report.findings)

    salvaged = verify(out, salvage=True)
    assert salvaged.ok, salvaged.findings
    assert any("trades-" in p for p in salvaged.salvaged)
    assert find_parts(out) == []
    after = Manifest.load(out).segments
    assert after[: len(finished)] == finished
    recovered = [s for s in after if s.recovered]
    assert recovered and all(s.rows > 0 for s in recovered)
    for seg in recovered:
        if seg.kind == "trades":
            assert check_tickreplay_trades(out / seg.path) == []

    final = verify(out)
    assert final.ok and not [f for f in final.findings if f.level == "warning"]


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")
@pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGTERM])
def test_signal_shutdown_flushes_and_closes(tmp_path: Path, sig: signal.Signals) -> None:
    out = tmp_path / "data"
    with ThreadedFakeExchange(stream_forever) as exchange:
        proc = start_recorder(exchange.url, out)
        try:
            wait_for(lambda: trade_segments(out) >= 1)
            os.kill(proc.pid, sig)
            _, err = proc.communicate(timeout=20)
        finally:
            if proc.poll() is None:
                proc.kill()
    assert proc.returncode == 0, err
    assert b"flushed and closed" in err
    assert find_parts(out) == []
    report = verify(out)
    assert report.ok and not [f for f in report.findings if f.level == "warning"]


def test_record_cli_in_process(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    out = tmp_path / "data"
    with ThreadedFakeExchange(stream_forever) as exchange:
        code = main(
            [
                "record",
                "--source",
                "binance",
                "--symbols",
                "btcusdt",
                "--out",
                str(out),
                "--url",
                exchange.url,
                "--duration",
                "1.5",
                "--bars",
                "1s",
                "--rotate-time",
                "1h",
            ]
        )
    assert code == 0
    err = capsys.readouterr().err
    assert "flushed and closed" in err and "1 connect(s)" in err
    report = verify(out)
    assert report.ok, report.findings
    kinds = {s.kind for s in Manifest.load(out).segments}
    assert {"trades", "bars"} <= kinds


def test_record_cli_backfill_note_and_no_bars(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    async def silent(conn: Conn, index: int) -> None:
        await conn.recv()
        await conn.drain()

    with ThreadedFakeExchange(silent) as exchange:
        code = main(
            [
                "record",
                "--source",
                "kraken",
                "--symbols",
                "BTC/USD",
                "--out",
                str(tmp_path),
                "--url",
                exchange.url,
                "--duration",
                "0.3",
                "--bars",
                "none",
                "--backfill",
            ]
        )
    assert code == 0
    assert "no documented public backfill" in capsys.readouterr().err


def test_record_cli_rejects_bad_options(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    args = ["record", "--source", "binance", "--symbols", "BTCUSDT", "--out", str(tmp_path)]
    assert main([*args, "--rotate-size", "lots"]) == 2
    assert "bad size" in capsys.readouterr().err
    assert main([*args[:4], "", "--out", str(tmp_path)]) == 2
