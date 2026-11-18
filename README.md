# market-data-recorder

[![CI](https://github.com/projectbin07-stack/market-data-recorder/actions/workflows/ci.yml/badge.svg)](https://github.com/projectbin07-stack/market-data-recorder/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A gap-aware recorder for **public** market-data WebSocket feeds that writes replayable,
checksummed, validated trade and bar files.

> **Scope and disclaimer.** market-data-recorder connects only to public,
> unauthenticated market-data streams. It has no API-key, account, order-placement,
> wallet or private-key code of any kind. It records what an exchange publishes and
> reports what it failed to record; the recorded data and the statistics it prints are
> not investment information or advice. Exchange terms govern what you may do with data
> you record - see [docs/sources.md](docs/sources.md). No recorded data is included in
> this repository.

## Why it exists

A recorder that silently drops a minute of trades during a reconnect produces files that
look fine and backtest wrong. This project treats every hole as data:

- **Gaps are written down, never papered over.** Disconnects, trade-id jumps, heartbeat
  ids ahead of what arrived, clock skew and out-of-order prints each produce a record in
  a sidecar file with a start, an end, a reason and - where the source has trade ids -
  the exact range of missing ids (or proof that none were lost).
- **Files are either complete or obviously not.** Segments are written as `.part`,
  fsynced, renamed into place and listed in a manifest with SHA-256 checksums. A killed
  process leaves at most one `.part` per stream, which `mdrec verify` reports and
  `--salvage` recovers line by line.
- **Output is replayable as-is.** Trade and bar CSVs follow the input rules of
  [tickreplay](https://github.com/projectbin07-stack/tickreplay) (the same author's
  backtesting engine), and the test suite loads recorded files with tickreplay itself.

## Features

- **Sources** (public trade streams, one adapter interface): Binance spot
  `<symbol>@trade`, Coinbase Exchange `matches` + `heartbeat`, Kraken WebSocket v2
  `trade`. Each normalises to a common `Trade` (exchange timestamp, local receive
  timestamp, `Decimal` price and size, taker side, trade id).
- **Connection management** on asyncio: protocol ping/pong per source spec, Kraken
  application pings, idle timeout (Coinbase and Kraken send heartbeats every second),
  reconnect with jittered exponential backoff that resets after a stable connection,
  subscription replay after every reconnect, clean shutdown with flush on Ctrl+C.
- **Gap awareness**: trade-id continuity per symbol; Coinbase heartbeat `last_trade_id`
  checks; disconnect windows with lost-trade counts; clock-skew windows; out-of-order
  and duplicate detection.
- **Optional REST backfill** (`--backfill`) of exactly the missing trade ids from
  documented public endpoints (Binance `historicalTrades`, Coinbase
  `/products/{id}/trades`), written to separate segments with `backfilled=1`.
- **Storage**: append-only segments rotated by size and age, atomic rename, a manifest
  with checksums, row counts and time/id ranges.
- **Bars** built locally from trades (any interval, several at once), emitted only when
  the interval closes, marked `partial=1` when they may be missing trades.
- **Tools**: `mdrec verify` (checksums, tickreplay rules, ordering, every id jump
  accounted for, gap report), `mdrec bars` (rebuild bars offline), `mdrec stats`
  (trade rate, exchange-to-local latency percentiles, gap totals).
- One runtime dependency, [`websockets`](https://pypi.org/project/websockets/); the rest
  is the standard library.

## Install

Requires Python 3.10 or newer. Not published on PyPI; install from source:

```bash
pip install "git+https://github.com/projectbin07-stack/market-data-recorder.git@v0.1.0"
```

or, for development, `pip install -e ".[dev]"` in a clone.

## Quick start

```bash
mdrec record --source coinbase --symbols BTC-USD,ETH-USD --out data/ --duration 3m
mdrec verify data/
mdrec stats data/
mdrec bars data/ --interval 1m
```

Symbols use each source's spelling: `BTCUSDT` (Binance), `BTC-USD` (Coinbase),
`BTC/USD` (Kraken). Without `--duration` the recorder runs until Ctrl+C, then flushes and
closes every segment.

The output below is exactly what these commands printed for a real three-minute Coinbase
recording on 2026-11-18 (UTC); the recorded data itself is not committed. Timestamps in
the log are local time (UTC+05:30).

```console
$ mdrec record --source coinbase --symbols BTC-USD,ETH-USD --out c --duration 180 --backfill
2026-11-19 04:37:29,745 INFO coinbase: connected to wss://ws-feed.exchange.coinbase.com, subscribed BTC-USD,ETH-USD
2026-11-19 04:37:29,995 INFO coinbase: subscriptions: [{"name": "matches", "product_ids": ["BTC-USD", "ETH-USD"], "account_ids": null}, {"name": "heartbeat", "product_ids": ["BTC-USD", "ETH-USD"], "account_ids": null}]
flushed and closed c: 3159 trades, 8 bars, 0 gap record(s), 0 backfilled, 0 duplicate/late id(s) dropped, 0 malformed message(s), 1 connect(s)

$ mdrec verify c
verified 4 segment(s), 3167 row(s) under c
gap report: no gap records
OK

$ mdrec stats c
coinbase BTC-USD
  live trades        2630
  receive span       178.5 s
  trades/s           14.74
  latency ms         p50 -1619.3, p90 -1582.8, p99 -1278.8, min -1628.0, max -1277.7
  negative latency   2630 trade(s) (clock offset)
coinbase ETH-USD
  live trades        529
  receive span       176.4 s
  trades/s           3.00
  latency ms         p50 -1622.6, p90 -1611.6, p99 -1518.5, min -1627.6, max -1466.7
  negative latency   529 trade(s) (clock offset)
gaps: 0 record(s)
latency = local receive time - exchange trade time; includes clock offset.
Operational statistics about the recording; not investment information.

$ mdrec bars c --interval 1m
coinbase BTC-USD: 4 bar(s) (2 partial) from 2630 trade(s) -> c\rebuilt\coinbase\BTC-USD\bars-1m.csv
coinbase ETH-USD: 4 bar(s) (2 partial) from 529 trade(s) -> c\rebuilt\coinbase\ETH-USD\bars-1m.csv
```

The negative latencies are not a bug: this machine's clock was measured 1.74 s *behind*
`time.windows.com` (`w32tm /stripchart`) right after the run, with the Windows time
service stopped. `stats` reports receive time minus exchange time as observed and does
not correct for clock offset. See [Live check](#live-check) for all three sources.

### Files

```
data/
  manifest.json                                   segment list with SHA-256, rows, ranges
  coinbase/
    gaps-<UTC>-<session>-0001.jsonl               gap and backfill records (sidecar)
    BTC-USD/
      trades-<UTC>-<session>-0001.csv             live trades
      trades-backfill-<UTC>-<session>-0001.csv    REST backfill (only with --backfill)
      bars-1m-<UTC>-<session>-0001.csv            bars built live
  rebuilt/coinbase/BTC-USD/bars-1m.csv            written by `mdrec bars`
```

A trade segment (values below are invented, as in the tests):

```csv
# market-data-recorder kind=trades source=binance symbol=BTCUSDT session=a1b2c3
# Recorded public market data. Not investment information or advice.
# timestamp=exchange trade time (UTC); recv_timestamp=local receive time; side=taker side
timestamp,symbol,price,size,side,trade_id,recv_timestamp,backfilled
2026-01-01T00:00:05.000000Z,BTCUSDT,101,1,buy,1,2026-01-01T00:00:05.050000Z,0
```

A gap record (abbreviated):

```json
{"type": "gap", "gap_id": "a1b2c3-BTCUSDT-2", "source": "binance", "symbol": "BTCUSDT",
 "reason": "sequence_jump", "start": "2026-01-01T00:01:40.000000Z",
 "end": "2026-01-01T00:01:50.000000Z", "first_missing_id": 21, "last_missing_id": 21,
 "missing": 1, "detail": "trade id jumped from 20 to 22"}
```

Bar files have the columns
`timestamp,symbol,open,high,low,close,volume,trades,partial,first_trade_id,last_trade_id`,
where `timestamp` is the bar's open time. tickreplay's `read_trades` / `read_bars` load
both directly (they keep the `#` lines as data notes and ignore the extra columns).

### CLI reference

```console
$ mdrec --help
usage: mdrec [-h] [--version] {record,verify,bars,stats} ...

Record public, unauthenticated market-data WebSocket feeds into gap-aware, checksummed
CSV segments that tickreplay reads without conversion. No API keys, no account
endpoints, no orders. Recorded data is not investment information.

positional arguments:
  {record,verify,bars,stats}
    record              record a public trade stream until Ctrl+C
    verify              check checksums, ordering, tickreplay rules and gaps
    bars                rebuild OHLCV bars from recorded trades
    stats               trade rate, exchange->local latency, gap totals

options:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
```

```console
$ mdrec record --help
usage: mdrec record [-h] --source {binance,coinbase,kraken} --symbols SYMBOLS
                    --out OUT [--bars BARS] [--rotate-size ROTATE_SIZE]
                    [--rotate-time ROTATE_TIME] [--duration DURATION]
                    [--max-skew MAX_SKEW] [--backfill]
                    [--backfill-max BACKFILL_MAX] [--url URL] [-v]

options:
  -h, --help            show this help message and exit
  --source {binance,coinbase,kraken}
  --symbols SYMBOLS     comma-separated, in the source's spelling (BTCUSDT /
                        BTC-USD / BTC/USD)
  --out OUT             output directory
  --bars BARS           comma-separated bar intervals built live, e.g. 1m,5m;
                        'none' disables (default 1m)
  --rotate-size ROTATE_SIZE
                        rotate segments at this size
  --rotate-time ROTATE_TIME
                        rotate segments at this age
  --duration DURATION   stop after this long (e.g. 300 or 5m)
  --max-skew MAX_SKEW   seconds of |receive - exchange time| before a
                        clock_skew record (default 5)
  --backfill            fill trade-id gaps from the source's public REST
                        endpoint (binance, coinbase)
  --backfill-max BACKFILL_MAX
                        max trades per gap
  --url URL             override the WebSocket URL (testing)
  -v, --verbose         debug logging
```

`mdrec verify PATH [--salvage] [--json]` exits 1 if any error is found.
`mdrec bars PATH [--interval 1m] [--out DIR]` and `mdrec stats PATH [--json]` complete
the set.

## Architecture

```
  exchange (public WSS)                                          local disk
        |
        v
  connection.run_feed   connect, subscribe (replayed on every reconnect),
        |               ping/pong, app ping, idle timeout, jittered backoff
        |  on_message(raw, recv_us) / on_state(up, reason)
        v
  adapters.<source>.parse  ->  Trade | Heartbeat | Notice
        v
  Recorder, per symbol:
    GapTracker.on_trade / on_heartbeat / on_disconnect -> GapRecord --> gaps-*.jsonl
    SegmentWriter(trades) ---------------------------------------------> trades-*.csv
    BarBuilder(interval) -> closed bars -> SegmentWriter --------------> bars-*.csv
    backfill worker (thread) -> adapter.backfill (public REST) --------> trades-backfill-*.csv
  every SegmentWriter.finalize (rename .part -> final) ----------------> manifest.json
```

| Module | Role |
|---|---|
| `adapters/` | `SourceAdapter` interface and the Binance, Coinbase, Kraken adapters (pure: build messages, parse messages, page REST backfill) |
| `connection.py` | `run_feed`: connect/subscribe/read loop, liveness, `Backoff` |
| `gaps.py` | `GapTracker`: continuity rules that turn holes into `GapRecord`s |
| `bars.py` | `BarBuilder` (live, interval-close) and `build_bars` (offline) |
| `storage.py` | `SegmentWriter`, `Manifest`, salvage of `.part` files |
| `csvformat.py` | CSV layouts and a re-implementation of tickreplay's input rules |
| `recorder.py` | wires the above together; flush, rotation, shutdown |
| `verify.py`, `stats.py`, `rebuild.py`, `cli.py` | the `mdrec` tools |

**Gap rules.** With trade ids, a disconnect window runs from the last trade before the
drop to the first trade (or id-bearing heartbeat) after it, and records how many ids are
missing - `0` when the reconnect proved nothing was lost. A jump on a live connection is
a `sequence_jump` with the exact missing range. A trade id below the last one is dropped
and recorded as `out_of_order`; an exact duplicate is dropped and counted. A trade whose
receive time differs from its exchange time by more than `--max-skew` (default 5 s)
opens a `clock_skew` window that is recorded when it closes. Connection attempts that
fail before any connection was up do not create gap records (nothing was being
recorded). `mdrec verify` checks that every trade-id jump in the files is covered by gap
records.

**Partial bars.** A bar is `partial=1` if it is the first bar of a recording session,
overlaps a gap that lost (or may have lost) trades, was in progress when the connection
dropped, or was flushed at shutdown before its interval closed. Intervals with no trades
produce no bar.

**Crash safety.** Rows are flushed and fsynced every second. A segment only gets its
final name, and only enters the manifest, after it is complete; the manifest is replaced
atomically. Killing the process mid-write therefore cannot corrupt a listed segment. The
test suite does exactly that: it starts `mdrec record` as a subprocess against a local
fake exchange, kills it once segments have rotated, and checks that `verify` passes,
reports the leftover `.part`, and that `--salvage` recovers its whole rows.

## Live check

Short real recordings were run on 2026-11-18 (UTC) from the author's Windows 11 machine
to confirm the adapters against the live services: each run was `--duration 180` with
two symbols. The data was verified with `mdrec verify`, loaded with tickreplay's
`read_trades` / `read_bars`, and then deleted; none of it is committed.

| Source (start, UTC) | Symbol | Live trades | Receive span | Trades/s | Latency p50 / p90 / p99 (ms) | Gap records |
|---|---|---:|---:|---:|---|---|
| Coinbase (23:07:28) | BTC-USD | 2,630 | 178.5 s | 14.74 | -1619.3 / -1582.8 / -1278.8 | none |
| Coinbase (23:07:28) | ETH-USD | 529 | 176.4 s | 3.00 | -1622.6 / -1611.6 / -1518.5 | none |
| Binance (23:11:10) | BTCUSDT | 4,290 | 178.1 s | 24.09 | -1677.5 / -1673.8 / -1602.0 | none |
| Binance (23:11:10) | ETHUSDT | 5,125 | 176.1 s | 29.11 | -1678.2 / -1674.8 / -1624.5 | none |
| Kraken (23:11:10) | BTC/USD | 4 | 28.2 s | 0.14 | -1632.6 / -1603.5 / -1603.5 | 2 disconnect (one with 117 trades known missing) |
| Kraken (23:11:10) | ETH/USD | 3 | 0.0 s | n/a | -1635.3 / -1635.3 / -1635.3 | 2 disconnect (loss unknown) |

All runs passed `mdrec verify`, and every trade and bar file loaded in tickreplay. The
Coinbase and Binance runs each had one connection for the whole run and no gaps (a
Binance run started at 23:07:28 at the same time as the Coinbase one only connected after
about 2.5 minutes of handshake timeouts and is not in the table). **The Kraken check is
inconclusive:** the adapter subscribed and parsed live trades, but connection attempts
were mostly reset by the network, and the connections that did come up went silent
(no trades and no heartbeats) within seconds, so the 10 s idle timeout closed them. Each
of those windows is in the gap file, including an exact count of 117 lost BTC/USD trades
computed from Kraken trade ids across one reconnect.

Notes on these numbers:

- **Latency is dominated by clock offset.** The local clock was 1.74 s behind
  `time.windows.com` (the Windows time service was stopped), so every "latency" is
  negative by roughly that amount. The spread between percentiles is meaningful; the
  absolute values are not a network-latency measurement.
- **The network path to Binance and Kraken was unreliable from this machine.** Many
  connection attempts failed with opening-handshake timeouts (Binance) or `WinError 64`
  connection resets (Kraken); a minimal standalone `websockets` script failed the same
  way, so it is the network path, not the adapters. The recorder backed off as designed
  and wrote a gap record for every window it lost after a connection had been up.
- Trade rates are per second of the receive span shown, which is shorter than 180 s when
  the connection came up late.

## Testing

```bash
pip install -e ".[dev]"
pip install "tickreplay @ git+https://github.com/projectbin07-stack/tickreplay.git@v0.1.0"  # optional
ruff check .
ruff format --check .
mypy
pytest --cov
```

No test talks to an exchange. The suite covers:

- adapters against messages written by hand from each documented schema (including
  malformed input, Decimal precision of Kraken's JSON numbers, Coinbase maker/taker side,
  Binance millisecond/microsecond times) and REST backfill pagination with an injected
  HTTP function;
- a local asyncio WebSocket server (`tests/fakeserver.py`) for reconnect and backoff,
  subscription replay, idle/heartbeat timeouts, application pings, disconnect and
  sequence-jump gap records, backfill, cancellation flush and `--duration`;
- the gap tracker and bar builder against hand-computed windows and OHLCV;
- segment rotation by size and time, atomic finalisation, manifest checksums, tamper
  detection and salvage of torn `.part` files;
- a subprocess killed mid-write, then verified and salvaged; SIGINT/SIGTERM shutdown on
  POSIX;
- tickreplay compatibility: recorded trades and bars are loaded with tickreplay's own
  `read_trades` / `read_bars` (installed into the project venv from its local checkout
  during development and from its `v0.1.0` tag in CI, test-only; CI sets
  `MDREC_REQUIRE_TICKREPLAY=1` so these tests cannot silently skip), and the
  re-implemented rule checks are compared with tickreplay on deliberately bad files.

CI runs everything on Ubuntu and Windows with Python 3.10 and 3.13 and fails below 85%
coverage.

## Known limitations

- **Trades only.** Order books, tickers and exchange-provided klines are not recorded;
  bars are built locally from trades.
- **One source and one connection per process**, up to the source's per-connection
  symbol limit. Run several processes for several sources. Two recorders must not share
  an output directory (there is no lock file).
- **Backfill** exists only for Binance and Coinbase, is capped per gap
  (`--backfill-max`, default 10,000), and is not retried after an HTTP error (the error
  is written to the gap file). Coinbase backfill pages from the newest missing id, so a
  capped backfill keeps the newest part of the gap. Live backfill has not been exercised:
  no id gap occurred during the live runs while a backfill-capable source was connected.
- **Kraken heartbeats carry no trade id**, so Kraken loss is detected from trade-id
  continuity only once the next trade arrives.
- **Clock skew is detected, not corrected.** Latency statistics are receive time minus
  exchange time as observed.
- **Bar closing uses the local clock** plus a 2 s grace period when no later trade
  arrives; a trade arriving after its bar was emitted is counted as late and not merged
  (it is still in the trade file, and `mdrec bars` includes it).
- **`mdrec stats` and `mdrec bars` load all trades of a stream into memory.** Fine for
  hours of data on a few symbols; not designed for months of tick data.
- **Windows:** Ctrl+C shutdown is handled (asyncio cancels the main task and the recorder
  flushes), but the automated signal tests only run on POSIX.
- Live operation has only been exercised in the short runs listed above; there has been
  no long-running (24 h+) soak test.

## Roadmap

Ideas, not commitments:

- Order-book (L2) snapshots and diffs with sequence checks.
- A lock file and multi-source orchestration in one process.
- Parquet output alongside CSV.
- A long-running soak test across Binance's 24 h forced reconnect.

## License

MIT - see [LICENSE](LICENSE). Copyright (c) 2026 Navaneeth KG.
