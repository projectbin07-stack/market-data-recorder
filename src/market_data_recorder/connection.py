"""WebSocket connection management: connect, subscribe, read, detect silence, reconnect.

One :func:`run_feed` call keeps one connection alive until it is cancelled. After every
(re)connect it sends the adapter's subscription messages again (subscription replay), so
a reconnect resumes the same streams. Liveness is checked three ways: protocol ping/pong
(handled by the ``websockets`` client with the adapter's interval and deadline), an
optional application-level ping, and an idle timeout that treats a silent connection as
dead. Reconnect delays use exponential backoff with jitter and reset once a connection
has stayed up for a while, so a flapping endpoint is not hammered.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import WebSocketException

from market_data_recorder.adapters.base import USER_AGENT, SourceAdapter
from market_data_recorder.timeutil import now_us

log = logging.getLogger("market_data_recorder.connection")


class IdleTimeout(Exception):
    """No message arrived within the adapter's idle timeout."""


@dataclass
class Backoff:
    """Exponential backoff with "equal jitter".

    The n-th consecutive failure waits ``d/2 + uniform(0, d/2)`` seconds where
    ``d = min(cap, base * 2**n)``: at least half the nominal delay (so reconnects never
    burst), at most the nominal delay, randomised so many clients do not synchronise.
    """

    base: float = 1.0
    cap: float = 60.0
    rng: random.Random = field(default_factory=random.Random)
    attempt: int = 0

    def nominal(self, attempt: int) -> float:
        return min(self.cap, self.base * (2.0 ** min(attempt, 60)))

    def next_delay(self) -> float:
        nominal = self.nominal(self.attempt)
        self.attempt += 1
        return nominal / 2 + self.rng.uniform(0, nominal / 2)

    def reset(self) -> None:
        self.attempt = 0


@dataclass
class FeedStats:
    """Counters about the connection itself (not about market data)."""

    connects: int = 0
    disconnects: int = 0
    failed_attempts: int = 0
    messages: int = 0
    idle_timeouts: int = 0
    last_error: str = ""
    delays: list[float] = field(default_factory=list)


OnMessage = Callable[[str | bytes, int], None]
OnState = Callable[[bool, str], None]
Sleep = Callable[[float], Awaitable[None]]


async def run_feed(
    adapter: SourceAdapter,
    symbols: Sequence[str],
    *,
    url: str | None = None,
    on_message: OnMessage,
    on_state: OnState,
    backoff: Backoff | None = None,
    stable_after: float = 30.0,
    open_timeout: float = 10.0,
    idle_timeout: float | None = None,
    stats: FeedStats | None = None,
    sleep: Sleep = asyncio.sleep,
    connect: Callable[..., Any] = ws_connect,
) -> None:
    """Run until cancelled; ``on_state(True, "")`` after subscribing, ``(False, why)`` on loss."""
    backoff = backoff or Backoff()
    stats = stats or FeedStats()
    target = url or adapter.default_url
    idle = idle_timeout if idle_timeout is not None else adapter.idle_timeout
    while True:
        connected_at: float | None = None
        reason = "connection closed"
        try:
            async with connect(
                target,
                ping_interval=adapter.ping_interval,
                ping_timeout=adapter.ping_timeout,
                open_timeout=open_timeout,
                close_timeout=5,
                max_size=2**22,
                user_agent_header=USER_AGENT,
            ) as ws:
                for message in adapter.subscribe_messages(symbols):
                    await ws.send(message)
                stats.connects += 1
                connected_at = time.monotonic()
                log.info(
                    "%s: connected to %s, subscribed %s", adapter.name, target, ",".join(symbols)
                )
                on_state(True, "")
                pinger = (
                    asyncio.create_task(_app_pings(adapter, ws))
                    if adapter.app_ping_interval
                    else None
                )
                try:
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=idle)
                        except asyncio.TimeoutError:
                            stats.idle_timeouts += 1
                            raise IdleTimeout(f"no message for {idle:g} s") from None
                        stats.messages += 1
                        on_message(raw, now_us())
                finally:
                    if pinger is not None:
                        pinger.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await pinger
        except (OSError, asyncio.TimeoutError, WebSocketException, IdleTimeout) as exc:
            reason = f"{type(exc).__name__}: {exc}"
        stats.last_error = reason
        if connected_at is not None:
            # Only a connection that was up can lose data; failed attempts just retry.
            stats.disconnects += 1
            on_state(False, reason)
            if time.monotonic() - connected_at >= stable_after:
                backoff.reset()
        else:
            stats.failed_attempts += 1
        delay = backoff.next_delay()
        stats.delays.append(delay)
        log.warning("%s: %s; reconnecting in %.1f s", adapter.name, reason, delay)
        await sleep(delay)


async def _app_pings(adapter: SourceAdapter, ws: Any) -> None:
    interval = adapter.app_ping_interval
    assert interval is not None
    counter = 0
    while True:
        await asyncio.sleep(interval)
        counter += 1
        message = adapter.app_ping(counter)
        if message is not None:
            await ws.send(message)
