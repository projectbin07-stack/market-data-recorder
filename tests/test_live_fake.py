"""Connection behaviour against a local fake exchange: reconnect, backoff, subscription
replay, heartbeat/idle timeouts, application pings, gap detection and backfill."""

from __future__ import annotations

import asyncio
import json
import random
import socket
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, ClassVar

import pytest

from fakeserver import Conn, FakeExchange
from market_data_recorder.adapters import BinanceAdapter, CoinbaseAdapter, KrakenAdapter
from market_data_recorder.connection import Backoff, FeedStats, run_feed
from market_data_recorder.recorder import Recorder, RecorderConfig
from market_data_recorder.storage import Manifest, find_parts
from market_data_recorder.timeutil import now_us
from market_data_recorder.verify import verify
from samples import (
    BINANCE_SUB_ACK,
    COINBASE_SUBSCRIPTIONS,
    KRAKEN_HEARTBEAT,
    KRAKEN_SUB_ACK,
    binance_trade,
    coinbase_heartbeat,
    coinbase_match,
    kraken_trades,
)


def fast_backoff() -> Backoff:
    return Backoff(base=0.02, cap=0.1, rng=random.Random(7))


async def run_until(recorder: Recorder, done: Callable[[], bool], timeout: float = 15.0) -> None:
    task = asyncio.create_task(recorder.run())
    deadline = asyncio.get_running_loop().time() + timeout
    try:
        while not done():
            if task.done():
                task.result()
                raise AssertionError("recorder stopped early")
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError("condition not reached in time")
            await asyncio.sleep(0.01)
    finally:
        recorder.stop()
        await asyncio.wait_for(task, 10)


def gap_lines(out: Path) -> list[dict[str, Any]]:
    records = []
    for path in sorted(out.rglob("gaps-*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            data = json.loads(line)
            if data["type"] != "header":
                records.append(data)
    return records


# -- backoff ----------------------------------------------------------------------------


def test_backoff_is_bounded_exponential_with_jitter() -> None:
    backoff = Backoff(base=1.0, cap=8.0, rng=random.Random(0))
    nominal = [1, 2, 4, 8, 8, 8]
    delays = [backoff.next_delay() for _ in nominal]
    for d, n in zip(delays, nominal, strict=True):
        assert n / 2 <= d <= n
    assert len(set(delays)) == len(delays)  # jittered, not constant
    backoff.reset()
    assert backoff.next_delay() <= 1.0


def _closed_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
    return port


def test_failed_connects_back_off_without_reporting_disconnects() -> None:
    delays: list[float] = []
    states: list[bool] = []
    stats = FeedStats()

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)
        if len(delays) >= 4:
            raise asyncio.CancelledError

    async def main() -> None:
        await run_feed(
            BinanceAdapter(),
            ["BTCUSDT"],
            url=f"ws://127.0.0.1:{_closed_port()}/ws",
            on_message=lambda raw, ts: None,
            on_state=lambda up, why: states.append(up),
            backoff=Backoff(1.0, 60.0, rng=random.Random(3)),
            stats=stats,
            sleep=fake_sleep,
            open_timeout=2,
        )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(main())
    assert states == []  # never connected, so nothing could be lost
    assert stats.failed_attempts == 4 and stats.disconnects == 0
    for d, n in zip(delays, [1, 2, 4, 8], strict=True):
        assert n / 2 <= d <= n


def test_stable_connection_resets_backoff_and_replays_subscription() -> None:
    delays: list[float] = []
    states: list[bool] = []

    async def script(conn: Conn, index: int) -> None:
        await conn.recv()
        await conn.close()

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)
        if len(delays) >= 3:
            raise asyncio.CancelledError

    async def main() -> FakeExchange:
        async with FakeExchange(script) as exchange:
            with pytest.raises(asyncio.CancelledError):
                await run_feed(
                    CoinbaseAdapter(),
                    ["BTC-USD"],
                    url=exchange.url,
                    on_message=lambda raw, ts: None,
                    on_state=lambda up, why: states.append(up),
                    backoff=Backoff(1.0, 60.0, rng=random.Random(3)),
                    stable_after=0.0,
                    sleep=fake_sleep,
                )
        return exchange

    exchange = asyncio.run(main())
    assert states == [True, False] * 3
    assert all(0.5 <= d <= 1.0 for d in delays)  # reset to the base delay every time
    assert len(exchange.received) == 3
    for log in exchange.received:
        assert json.loads(log[0])["channels"] == ["matches", "heartbeat"]


# -- full recorder against the fake exchange --------------------------------------------


def test_reconnect_replays_subscription_and_records_gaps_with_backfill(tmp_path: Path) -> None:
    async def script(conn: Conn, index: int) -> None:
        await conn.recv()
        await conn.send(BINANCE_SUB_ACK)
        now = now_us()
        if index == 0:
            for tid in range(1, 6):
                await conn.send(binance_trade(tid, "100.5", "0.1", now + tid))
            await asyncio.sleep(0.05)
            await conn.close()  # drop: trades 6.. continue on the next connection
            return
        for tid in (6, 7, 8, 10, 11):  # 9 never arrives on the stream
            await conn.send(binance_trade(tid, "101", "0.2", now + tid))
        await conn.drain()

    requested: list[str] = []

    def http_get(url: str) -> tuple[Any, Mapping[str, str]]:
        requested.append(url)
        item = {
            "id": 9,
            "price": "100.9",
            "qty": "0.3",
            "quoteQty": "1",
            "time": now_us() // 1000,
            "isBuyerMaker": False,
            "isBestMatch": True,
        }
        return [item], {}

    async def main() -> tuple[Recorder, FakeExchange]:
        async with FakeExchange(script) as exchange:
            config = RecorderConfig(
                source="binance",
                symbols=["BTCUSDT"],
                out=tmp_path,
                url=exchange.url,
                flush_interval_s=0.02,
                backfill=True,
            )
            recorder = Recorder(config, BinanceAdapter(), backoff=fast_backoff(), http_get=http_get)
            await run_until(
                recorder,
                lambda: (
                    recorder.summary.trades["BTCUSDT"] == 10 and recorder.summary.backfilled == 1
                ),
            )
        return recorder, exchange

    recorder, exchange = asyncio.run(main())
    assert len(exchange.received) >= 2
    for log in exchange.received[:2]:
        assert json.loads(log[0]) == {"method": "SUBSCRIBE", "params": ["btcusdt@trade"], "id": 1}
    records = gap_lines(tmp_path)
    gaps = [r for r in records if r["type"] == "gap"]
    assert [(g["reason"], g["missing"]) for g in gaps] == [("disconnect", 0), ("sequence_jump", 1)]
    assert (gaps[1]["first_missing_id"], gaps[1]["last_missing_id"]) == (9, 9)
    (backfill,) = [r for r in records if r["type"] == "backfill"]
    assert backfill["complete"] and backfill["gap_id"] == gaps[1]["gap_id"]
    assert requested and "fromId=9" in requested[0]
    assert recorder.feed_stats.connects >= 2
    report = verify(tmp_path)
    assert report.ok, report.findings
    assert find_parts(tmp_path) == []
    backfill_files = list(tmp_path.rglob("trades-backfill-*.csv"))
    assert len(backfill_files) == 1
    assert ",9," in backfill_files[0].read_text(encoding="utf-8")


def test_idle_timeout_and_heartbeat_ids_detect_lost_trades(tmp_path: Path) -> None:
    async def script(conn: Conn, index: int) -> None:
        await conn.recv()
        await conn.send(COINBASE_SUBSCRIPTIONS)
        now = now_us()
        if index == 0:
            await conn.send(coinbase_match(100, "1", "1", now, kind="last_match"))
            await conn.send(coinbase_heartbeat(100, now))
            await conn.send(coinbase_match(101, "2", "1", now + 1))
            await conn.drain()  # then fall silent: the client must give up on its own
            return
        await conn.send(coinbase_match(103, "1", "1", now, kind="last_match"))  # 102-103 lost
        await conn.send(coinbase_heartbeat(105, now + 2))  # 104-105 never sent either
        await conn.send(coinbase_match(106, "3", "1", now + 3))
        await conn.drain()

    async def main() -> Recorder:
        async with FakeExchange(script) as exchange:
            config = RecorderConfig(
                source="coinbase",
                symbols=["BTC-USD"],
                out=tmp_path,
                url=exchange.url,
                flush_interval_s=0.02,
                idle_timeout_s=0.3,
            )
            recorder = Recorder(config, CoinbaseAdapter(), backoff=fast_backoff())
            await run_until(recorder, lambda: recorder.summary.trades["BTC-USD"] == 2)
        return recorder

    recorder = asyncio.run(main())
    assert recorder.feed_stats.idle_timeouts >= 1
    gaps = [r for r in gap_lines(tmp_path) if r["type"] == "gap"]
    assert [(g["reason"], g["first_missing_id"], g["last_missing_id"]) for g in gaps] == [
        ("disconnect", 102, 103),
        ("sequence_jump", 104, 105),
    ]
    assert "IdleTimeout" in gaps[0]["detail"]
    report = verify(tmp_path)  # 101 -> 106 is covered by the two records together
    assert report.ok, report.findings


class QuickPingKraken(KrakenAdapter):
    app_ping_interval: ClassVar[float | None] = 0.05


def test_application_pings_are_sent_and_pongs_accepted(tmp_path: Path) -> None:
    async def script(conn: Conn, index: int) -> None:
        await conn.recv()
        await conn.send(KRAKEN_SUB_ACK)
        await conn.send(KRAKEN_HEARTBEAT)
        await conn.send(
            kraken_trades(
                [(1, "50000.1", "0.01", now_us(), "buy"), (2, "50000.2", "0.02", now_us(), "sell")]
            )
        )
        while True:
            message = json.loads(await conn.recv())
            if message.get("method") == "ping":
                await conn.send(json.dumps({"method": "pong", "req_id": message["req_id"]}))

    async def main() -> FakeExchange:
        async with FakeExchange(script) as exchange:
            config = RecorderConfig(
                source="kraken",
                symbols=["BTC/USD"],
                out=tmp_path,
                url=exchange.url,
                flush_interval_s=0.02,
            )
            recorder = Recorder(config, QuickPingKraken(), backoff=fast_backoff())

            def pinged() -> bool:
                return (
                    bool(exchange.received)
                    and sum('"ping"' in m for m in exchange.received[0]) >= 3
                )

            await run_until(recorder, lambda: pinged() and recorder.summary.trades["BTC/USD"] == 2)
        return exchange

    exchange = asyncio.run(main())
    pings = [json.loads(m) for m in exchange.received[0][1:]]
    assert [p["req_id"] for p in pings[:3]] == [1001, 1002, 1003]
    assert verify(tmp_path).ok
    assert list(tmp_path.glob("kraken/BTC_USD/trades-*.csv"))


def test_cancellation_flushes_and_finalises_everything(tmp_path: Path) -> None:
    async def script(conn: Conn, index: int) -> None:
        await conn.recv()
        tid = 0
        while True:
            tid += 1
            await conn.send(binance_trade(tid, "10", "1", now_us()))
            await asyncio.sleep(0.002)

    async def main() -> Recorder:
        async with FakeExchange(script) as exchange:
            config = RecorderConfig(
                source="binance",
                symbols=["BTCUSDT"],
                out=tmp_path,
                url=exchange.url,
                rotate_bytes=4096,
            )
            recorder = Recorder(config, BinanceAdapter(), backoff=fast_backoff())
            task = asyncio.create_task(recorder.run())
            while recorder.summary.trades["BTCUSDT"] < 100:  # noqa: ASYNC110
                await asyncio.sleep(0.01)
            task.cancel()  # what Ctrl+C does to the main task under asyncio.run
            with pytest.raises(asyncio.CancelledError):
                await task
        return recorder

    recorder = asyncio.run(main())
    assert find_parts(tmp_path) == []
    report = verify(tmp_path)
    assert report.ok, report.findings
    segments = Manifest.load(tmp_path).segments
    written = sum(s.rows for s in segments if s.kind == "trades")
    assert written == recorder.summary.trades["BTCUSDT"]
    assert recorder.summary.bars["BTCUSDT"] >= 1  # the open bar was flushed as partial


def test_duration_limit_stops_the_recorder(tmp_path: Path) -> None:
    async def script(conn: Conn, index: int) -> None:
        await conn.recv()
        await conn.drain()

    async def main() -> None:
        async with FakeExchange(script) as exchange:
            config = RecorderConfig(
                source="binance",
                symbols=["BTCUSDT"],
                out=tmp_path,
                url=exchange.url,
                duration_s=0.3,
            )
            await asyncio.wait_for(Recorder(config, BinanceAdapter()).run(), 10)

    asyncio.run(main())
    assert (tmp_path / "manifest.json").exists() is False  # nothing recorded, nothing listed
    assert find_parts(tmp_path) == []
