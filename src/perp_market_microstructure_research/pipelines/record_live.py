import asyncio
from pathlib import Path

from perp_market_microstructure_research.ingestion.adapters.binance.depth_recorder import (
    record_binance_depth,
)


def main():
    asyncio.run(
        record_binance_depth(
            symbol="BTCUSDT",
            out_dir=Path("data/binance/BTCUSDT/raw"),
            interval_ms=100,
        )
    )
