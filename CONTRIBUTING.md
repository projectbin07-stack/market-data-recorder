# Contributing

Bug reports, new public sources and pull requests are welcome.

## Development setup

```console
git clone https://github.com/projectbin07-stack/market-data-recorder.git
cd market-data-recorder
python -m venv .venv
.venv/bin/pip install -e ".[dev]"      # Windows: .venv\Scripts\pip
# optional, for the tickreplay compatibility tests (otherwise they skip):
.venv/bin/pip install "tickreplay @ git+https://github.com/projectbin07-stack/tickreplay.git@v0.1.0"
```

## Checks

All four must pass; CI runs them on Ubuntu and Windows with Python 3.10 and 3.13.

```console
ruff check .
ruff format --check .
mypy
pytest --cov
```

## Guidelines

- **Public data only.** No API keys, authenticated channels, account endpoints, order
  placement or wallet code. Pull requests adding any of these will be closed.
- **No live network in tests.** Tests run against `tests/fakeserver.py` (a local asyncio
  WebSocket server) and injected HTTP functions. CI must never talk to an exchange.
- **No recorded third-party data in the repository.** Sample messages are written by hand
  from the documented schema, with invented values; cite the documentation page in the
  adapter docstring and in `docs/sources.md`.
- **A new source** needs: an adapter implementing `SourceAdapter`, hand-written sample
  messages, parser tests including malformed input, its published connection and rate
  limits in `docs/sources.md`, and a note on its data terms.
- **Never hide a gap.** Anything that can lose or distort data must produce a gap record
  and a test that shows it.
- Expected values in tests are worked out by hand, with the working in a comment.
- Keep commits small with imperative messages (`Add ...`, `Fix ...`).
