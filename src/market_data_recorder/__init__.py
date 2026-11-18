"""market-data-recorder: a gap-aware recorder for public market-data WebSocket feeds.

Records public, unauthenticated trade streams into append-only, checksummed CSV segments
that tickreplay can read without conversion, and writes every detected gap to a sidecar
file instead of hiding it.
"""

from market_data_recorder.models import Bar, GapRecord, Heartbeat, Notice, Trade

__version__ = "0.1.0"

__all__ = ["Bar", "GapRecord", "Heartbeat", "Notice", "Trade", "__version__"]
