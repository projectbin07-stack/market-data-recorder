# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-11-19

First release.

### Added

- Source adapters for public, unauthenticated trade streams: Binance spot `<symbol>@trade`,
  Coinbase Exchange `matches` + `heartbeat`, Kraken WebSocket v2 `trade`. All normalise to
  one `Trade` model with exchange and local receive timestamps (integer microseconds),
  `Decimal` prices and sizes, taker side and trade id.
- asyncio connection manager: protocol ping/pong, Kraken application pings, idle timeout,
  reconnect with jittered exponential backoff that resets after a stable connection, and
  subscription replay after every reconnect.
- Gap tracking: `disconnect`, `sequence_jump` (from trade ids and Coinbase heartbeat
  `last_trade_id`), `clock_skew` and `out_of_order` records in an append-only JSONL
  sidecar, with exact missing-id ranges where the source provides ids.
- Optional REST backfill of missing trade ids for Binance (`historicalTrades`) and
  Coinbase (`/products/{id}/trades`), written to separate segments marked `backfilled=1`.
- Storage: append-only CSV/JSONL segments rotated by size and age, written as `.part` and
  renamed into place, with a `manifest.json` (SHA-256, size, rows, time and id range)
  replaced atomically.
- tickreplay-compatible trade and bar CSV; bars built live from trades, emitted only when
  their interval closes, and marked `partial` when they may be missing trades.
- `mdrec record | verify | bars | stats`, including `verify --salvage` to recover whole
  rows from a segment left behind by a crash.

[0.1.0]: https://github.com/projectbin07-stack/market-data-recorder/releases/tag/v0.1.0
