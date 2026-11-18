"""A local asyncio WebSocket server that plays scripted exchange behaviour.

Each accepted connection runs ``script(conn, index)`` where ``index`` counts connections
from 0. Everything the client sends is logged per connection, so tests can assert that
subscriptions were replayed after a reconnect.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
from collections.abc import Awaitable, Callable
from typing import Any

from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed


class Conn:
    def __init__(self, ws: ServerConnection, log: list[str]) -> None:
        self.ws = ws
        self.log = log

    async def recv(self) -> str:
        message = await self.ws.recv()
        text = message if isinstance(message, str) else message.decode()
        self.log.append(text)
        return text

    async def send(self, message: str) -> None:
        await self.ws.send(message)

    async def drain(self) -> None:
        """Log client messages (pings, subscriptions) until the client goes away."""
        try:
            while True:
                await self.recv()
        except ConnectionClosed:
            return

    async def close(self) -> None:
        await self.ws.close()


Script = Callable[[Conn, int], Awaitable[None]]


class FakeExchange:
    def __init__(self, script: Script) -> None:
        self.script = script
        self.received: list[list[str]] = []
        self.server: Server | None = None
        self.url = ""

    async def _handler(self, ws: ServerConnection) -> None:
        log: list[str] = []
        self.received.append(log)
        index = len(self.received) - 1
        with contextlib.suppress(ConnectionClosed):
            await self.script(Conn(ws, log), index)

    async def __aenter__(self) -> FakeExchange:
        self.server = await serve(self._handler, "127.0.0.1", 0)
        port = self.server.sockets[0].getsockname()[1]
        self.url = f"ws://127.0.0.1:{port}/ws"
        return self

    async def __aexit__(self, *exc: Any) -> None:
        assert self.server is not None
        self.server.close()
        await self.server.wait_closed()


class ThreadedFakeExchange:
    """Run a :class:`FakeExchange` on its own event loop in a thread (for subprocess tests)."""

    def __init__(self, script: Script) -> None:
        self.exchange = FakeExchange(script)
        self.loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.exchange.__aenter__())
        self._ready.set()
        self.loop.run_forever()

    def __enter__(self) -> FakeExchange:
        self._thread.start()
        self._ready.wait(10)
        return self.exchange

    def __exit__(self, *exc: Any) -> None:
        future = asyncio.run_coroutine_threadsafe(self.exchange.__aexit__(), self.loop)
        try:
            future.result(10)
        finally:
            self.loop.call_soon_threadsafe(self.loop.stop)
            self._thread.join(10)
