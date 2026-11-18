"""The interface every public-feed source adapter implements."""

from __future__ import annotations

import json
import urllib.request
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from decimal import Decimal
from typing import Any, ClassVar

from market_data_recorder.models import Event, Trade

#: ``get(url) -> (decoded JSON body, response headers)``; injectable for tests.
HttpGet = Callable[[str], tuple[Any, Mapping[str, str]]]

USER_AGENT = (
    "market-data-recorder/0.1 (+https://github.com/projectbin07-stack/market-data-recorder)"
)


class AdapterError(ValueError):
    """A message could not be interpreted according to the source's documented schema."""


def http_get_json(url: str, timeout: float = 10.0) -> tuple[Any, Mapping[str, str]]:
    """Plain HTTPS GET of a public, unauthenticated JSON endpoint."""
    request = urllib.request.Request(
        url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = json.loads(response.read().decode("utf-8"))
        return body, dict(response.headers.items())


def decimal_field(value: object, name: str) -> Decimal:
    """Convert a JSON number or numeric string to ``Decimal`` without going through float."""
    if isinstance(value, bool) or value is None:
        raise AdapterError(f"{name} is missing or not a number: {value!r}")
    if isinstance(value, (Decimal, int)):
        return Decimal(value)
    if isinstance(value, str):
        try:
            result = Decimal(value)
        except ArithmeticError:
            raise AdapterError(f"{name} is not a number: {value!r}") from None
        if not result.is_finite():
            raise AdapterError(f"{name} is not finite: {value!r}")
        return result
    raise AdapterError(f"{name} has unexpected type {type(value).__name__}")


def int_field(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise AdapterError(f"{name} is not an integer: {value!r}")
    return value


def str_field(data: Mapping[str, Any], name: str) -> str:
    value = data.get(name)
    if not isinstance(value, str) or not value:
        raise AdapterError(f"{name} is missing or not a string: {value!r}")
    return value


class SourceAdapter(ABC):
    """Translate one exchange's public WebSocket protocol into the common model.

    Adapters are pure: they build subscription messages and parse received text. All
    networking, timing and storage live elsewhere, which is what lets the same adapter be
    tested against hand-written sample messages and a local fake server.
    """

    #: Short source name used on the command line and in file paths.
    name: ClassVar[str]
    #: Public market-data WebSocket endpoint.
    default_url: ClassVar[str]
    #: Protocol-level ping cadence and pong deadline handed to the WebSocket client.
    ping_interval: ClassVar[float | None] = 20.0
    ping_timeout: ClassVar[float | None] = 20.0
    #: Reconnect if no message at all arrives for this long (``None`` disables).
    idle_timeout: ClassVar[float | None] = None
    #: Send :meth:`app_ping` this often (``None`` if the source has no application ping).
    app_ping_interval: ClassVar[float | None] = None
    #: Most symbols one connection may carry under the source's published limits.
    max_symbols: ClassVar[int] = 100
    #: Whether :meth:`backfill` is implemented against a documented public REST endpoint.
    supports_backfill: ClassVar[bool] = False

    def normalize_symbol(self, symbol: str) -> str:
        """Canonical symbol spelling for this source (used in files and messages)."""
        return symbol.strip().upper()

    @abstractmethod
    def subscribe_messages(self, symbols: Sequence[str]) -> list[str]:
        """Messages to send after every (re)connect to subscribe to ``symbols``' trades."""

    @abstractmethod
    def parse(self, raw: str | bytes, recv_us: int) -> list[Event]:
        """Turn one received message into events. Raises :class:`AdapterError`."""

    def app_ping(self, counter: int) -> str | None:
        """An application-level ping message, if the source defines one."""
        return None

    def backfill(
        self,
        symbol: str,
        first_missing_id: int,
        last_missing_id: int,
        *,
        get: HttpGet,
        recv_us: Callable[[], int],
        max_trades: int,
    ) -> list[Trade]:
        """Fetch trades with ids in ``[first_missing_id, last_missing_id]`` over public REST.

        Returned trades are marked ``backfilled=True`` and sorted by trade id.
        """
        raise NotImplementedError(f"{self.name} has no documented public backfill endpoint")


def load_json(raw: str | bytes, *, decimals: bool = False) -> Any:
    """Decode a message, optionally reading JSON floats as ``Decimal`` (no binary rounding)."""
    try:
        if decimals:
            return json.loads(raw, parse_float=Decimal)
        return json.loads(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise AdapterError(f"message is not valid JSON: {exc}") from None
