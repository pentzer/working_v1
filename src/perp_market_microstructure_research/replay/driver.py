from pathlib import Path

from perp_market_microstructure_research.core.orderbook.apply import apply_l2_delta
from perp_market_microstructure_research.core.orderbook.state import OrderBookState
from perp_market_microstructure_research.replay.loader import load_normalized_deltas


def replay_file(path: Path) -> OrderBookState:
    state = OrderBookState()
    for record in load_normalized_deltas(path):
        for price, size in record["b"]:
            apply_l2_delta(
                state,
                {
                    "price": price,
                    "size": size,
                    "side": "bids",
                },
            )
        for price, size in record["a"]:
            apply_l2_delta(
                state,
                {
                    "price": price,
                    "size": size,
                    "side": "asks",
                },
            )
    return state
