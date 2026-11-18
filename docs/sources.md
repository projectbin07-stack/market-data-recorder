# Sources, published limits and terms

market-data-recorder connects only to public, unauthenticated market-data endpoints. It
sends no API key, signature, passphrase or token to any of them, and it never calls an
account, order or wallet endpoint. This page lists, per source, the documentation the
adapter was written from, the published rules it follows, and what the terms say about
the recorded data.

The documentation was read on 2026-11-19. Exchanges change their APIs; if something
here disagrees with the current documentation, the documentation wins - please open an
issue.

## Recorded data is not committed or redistributed

No data recorded from any source is committed to this repository, used in tests, or
shipped in a release. The test fixtures in `tests/samples.py` are written by hand from
the documented schemas, with invented ids, prices and times. The terms below were
checked as far as they could be read; none of them clearly grants a right to redistribute
recorded data, so this project does not. If you record data with this tool, the
source's terms apply to what you do with it.

## Binance (spot) - `binance`

- WebSocket streams: <https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams>
  (also published as `web-socket-streams.md` in
  <https://github.com/binance/binance-spot-api-docs>)
- REST API: <https://developers.binance.com/docs/binance-spot-api-docs/rest-api>
  (`rest-api.md` in the same repository)

| Published rule | How the adapter follows it |
|---|---|
| Endpoint `wss://stream.binance.com:9443`; `wss://data-stream.binance.vision` serves market data only | default URL `wss://stream.binance.com:9443/ws` |
| Server pings every 20 s; no pong within 1 minute disconnects | the `websockets` client answers pings automatically |
| At most 5 incoming messages per second per connection, counting ping/pong frames and SUBSCRIBE | one SUBSCRIBE for all symbols per connection; client pings every 30 s |
| At most 1024 streams per connection | `max_symbols = 1024` |
| 300 connection attempts per 5 minutes per IP | reconnect backoff starts at 0.5-1 s, doubles, caps at 30-60 s |
| A connection is valid for 24 hours | the server's disconnect is handled as a normal reconnect with a gap record |
| `<symbol>@trade` payload fields `e,E,s,t,p,q,T,m,M`; `m` = buyer is maker | `t` is the trade id used for continuity, `T` the exchange time, taker side = `sell` if `m` |
| `timeUnit=MICROSECOND` switches time fields to microseconds | both units are accepted (told apart by magnitude) |
| `GET /api/v3/historicalTrades` (`symbol`, `fromId`, `limit` <= 1000, weight 25) needs no API key; `https://data-api.binance.vision` serves public market data | backfill pages by `fromId` on `data-api.binance.vision`, 0.5 s between pages, at most `--backfill-max` trades per gap |
| HTTP 429/418 with `Retry-After` when over the request-weight limit | a failed backfill request is recorded in the gap file (`error`) and not retried in a loop |

Terms: Binance Terms of Use, <https://www.binance.com/en/terms>. The Binance Vision
dataset terms (<https://data.binance.vision/Binance_Vision-Terms_of_Use.pdf>) describe
historical data as provided for computational, educational and research purposes. No
recorded Binance data is committed.

## Coinbase Exchange - `coinbase`

- WebSocket overview: <https://docs.cdp.coinbase.com/exchange/websocket-feed/overview>
- Channels: <https://docs.cdp.coinbase.com/exchange/websocket-feed/channels>
- WebSocket rate limits: <https://docs.cdp.coinbase.com/exchange/websocket-feed/rate-limits>
- Trades endpoint: <https://docs.cdp.coinbase.com/exchange/reference/exchangerestapi_getproducttrades>
- Pagination: <https://docs.cdp.coinbase.com/exchange/rest-api/pagination>
- REST rate limits: <https://docs.cdp.coinbase.com/exchange/rest-api/rate-limits>

| Published rule | How the adapter follows it |
|---|---|
| Unauthenticated market data at `wss://ws-feed.exchange.coinbase.com` | default URL; no credentials in the subscribe message |
| A `subscribe` must be sent within 5 s of connecting | sent immediately after every (re)connect |
| 8 connection requests/s per IP (burst 20); 100 client messages/s per IP | one subscribe message per connection; backoff on reconnect |
| `heartbeat` messages every second per product carry `last_trade_id` "to verify that no messages were missed" | subscribed with `matches`; a heartbeat id above the last received trade produces a `sequence_jump` gap record |
| The `matches` channel can drop messages; missed trades can be fetched from REST | `--backfill` fetches exactly the missing ids |
| The first message after subscribing is a `last_match` | used as the trade-id baseline only (it is an old trade, not live data) |
| `side` is the maker side | taker side stored = opposite of `side` |
| `GET /products/{id}/trades` needs no authentication; `after` pages to older trades via the `CB-AFTER` header; `limit` <= 1000 | backfill starts at `after = last_missing_id + 1` and follows `CB-AFTER` |
| Public REST: 10 requests/s per IP, burst 15 | 0.25 s between backfill pages |

Terms: Coinbase user agreement and market data terms,
<https://www.coinbase.com/legal/user_agreement> and
<https://www.coinbase.com/legal/market_data> (the latter could not be fetched by the
automated reader used while writing this page, so it was not reviewed in full). No
recorded Coinbase data is committed.

## Kraken (spot WebSocket v2) - `kraken`

- Trade channel: <https://docs.kraken.com/api/docs/websocket-v2/trade>
- Heartbeat: <https://docs.kraken.com/api/docs/websocket-v2/heartbeat>
- Ping: <https://docs.kraken.com/api/docs/websocket-v2/ping>
- WebSocket introduction (connection limits): <https://docs.kraken.com/api/docs/guides/spot-ws-intro>

| Published rule | How the adapter follows it |
|---|---|
| Public endpoint `wss://ws.kraken.com/v2`; the trade channel needs no token | default URL; subscribe has no token |
| `subscribe` with `channel: trade`, `symbol` list, optional `snapshot` (default false) | `snapshot: false`, so only live trades are recorded |
| `price` and `qty` are JSON numbers; `trade_id` is "sequence number, unique per book"; `timestamp` RFC 3339 | numbers decoded as `Decimal` (no float rounding); `trade_id` drives continuity checks |
| A heartbeat arrives about once a second when no other update is sent | idle timeout of 10 s treats a silent connection as dead |
| Application-level `ping` / `pong` with `req_id` | a `ping` every 30 s |
| About 150 connection attempts per rolling 10 minutes per IP before a 10-minute ban | reconnect backoff (0.5-1 s, doubling, cap 60 s) stays far below |

There is no Kraken backfill: the public REST `Trades` endpoint pages by time, so a
backfill could not be matched exactly to a range of missing trade ids.

Terms: Kraken Terms of Service, <https://www.kraken.com/legal>. No recorded Kraken data
is committed.

## Sources considered and not implemented

- **DexScreener-style polling APIs** were not implemented: polling aggregated pair data
  is not a trade stream with ids, so gap detection would be guesswork, and the terms of
  such aggregators were not reviewed.
