from perp_market_microstructure_research.core.orderbook.state import OrderBookState


def snapshot_l2(
    state: OrderBookState, *, include_metadata: bool = False
) -> dict[str, object]:
    bids = sorted(state.bids.items(), key=lambda kv: kv[0], reverse=True)
    asks = sorted(state.asks.items(), key=lambda kv: kv[0])

    snap: dict[str, object] = {"bids": bids, "asks": asks}
    if include_metadata and state.metadata is not None:
        snap["metadata"] = dict(state.metadata)
    return snap
