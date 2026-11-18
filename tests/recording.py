"""Build a small, fully known recording in-process (no network) for the tool tests.

Binance-format messages for BTCUSDT: trade ``i`` (1..30) happens at T0 + 5*i s at price
100 + i with size 1, and is received 50 ms later. The connection drops after trade 10
(trade 11 follows with no id lost), and trade 21 never arrives.
"""

from __future__ import annotations

from pathlib import Path

from market_data_recorder.adapters import BinanceAdapter
from market_data_recorder.recorder import Recorder, RecorderConfig
from samples import T0_US, binance_trade

S = 1_000_000
LATENCY_US = 50_000
IDS = [i for i in range(1, 31) if i != 21]


class Clock:
    def __init__(self) -> None:
        self.now = T0_US

    def __call__(self) -> int:
        return self.now


def make_recording(root: Path, **config: object) -> Recorder:
    clock = Clock()
    recorder = Recorder(
        RecorderConfig(source="binance", symbols=["BTCUSDT"], out=root, **config),  # type: ignore[arg-type]
        BinanceAdapter(),
        clock=clock,
    )
    recorder.on_message('{"result": null, "id": 1}', clock())
    for i in IDS:
        ts = T0_US + 5 * i * S
        clock.now = ts + LATENCY_US
        recorder.on_message(binance_trade(i, f"{100 + i}", "1", ts), clock.now)
        if i == 10:
            recorder.on_state(False, "ConnectionClosedError: test drop")
            recorder.on_state(True, "")
    recorder.on_message("{not json", clock())
    recorder.on_message(binance_trade(99, "1", "1", clock(), symbol="ETHUSDT"), clock())
    recorder.on_message('{"code": 2, "msg": "Invalid request"}', clock())
    recorder.close()
    return recorder
