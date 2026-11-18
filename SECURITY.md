# Security policy

## Supported versions

Only the latest release receives fixes.

## Reporting a vulnerability

Please do not open a public issue for security problems. Use GitHub's private reporting
instead: <https://github.com/projectbin07-stack/market-data-recorder/security/advisories/new>.

Include a description, the command or input that triggers the problem and the version you
used. Reports are handled on a best-effort basis by a single maintainer.

## Scope and design

- **Public data only.** The recorder connects only to public, unauthenticated market-data
  WebSocket streams and, for `--backfill`, public REST trade endpoints. There is no code
  for API keys, signatures, account or user-data streams, order placement, wallets or
  private keys, and none will be accepted. Do not put credentials in a `--url`.
- **Untrusted input.** Messages from the network are untrusted. They are parsed with the
  standard `json` module into the documented fields; anything else is counted as
  malformed and skipped. Message size is capped at 4 MiB by the WebSocket client.
- **Files.** The recorder writes only under the `--out` directory; `mdrec bars` writes
  under `<path>/rebuilt` or `--out`. `verify --salvage` rewrites only `*.part` files the
  recorder created (it refuses files without the recorder's header line).

Reports of interest include crashes or unbounded memory/disk use caused by crafted
messages, path traversal through symbol names (symbols are sanitised to
`[A-Za-z0-9._-]` for directory names), and anything that would cause data to be lost
without a gap record.
