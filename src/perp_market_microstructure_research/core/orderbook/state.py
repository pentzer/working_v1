from typing import Dict, Optional


def _normalize_side(side: str) -> str:
    if not isinstance(side, str):
        raise ValueError("side must be a string")
    s = side.lower()
    if s in ("bid", "bids", "b", "buy"):
        return "bids"
    if s in ("ask", "asks", "a", "sell"):
        return "asks"
    raise ValueError(f"unknown side: {side!r}")


def _side_map(state: "OrderBookState", side: str) -> Dict[int, int]:
    norm = _normalize_side(side)
    return state.bids if norm == "bids" else state.asks


class OrderBookState:
    def __init__(
        self,
        *,
        bids: Optional[Dict[int, int]] = None,
        asks: Optional[Dict[int, int]] = None,
        metadata: Optional[Dict[str, object]] = None,
    ) -> None:
        self.bids: Dict[int, int] = dict(bids) if bids is not None else {}
        self.asks: Dict[int, int] = dict(asks) if asks is not None else {}
        self.metadata: Optional[Dict[str, object]] = (
            dict(metadata) if metadata is not None else None
        )

    def best_bid(self) -> Optional[int]:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> Optional[int]:
        return min(self.asks) if self.asks else None

    def depth(self, side: str) -> int:
        return len(_side_map(self, side))

    def bids_desc(self) -> list[tuple[int, int]]:
        return sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)

    def asks_asc(self) -> list[tuple[int, int]]:
        return sorted(self.asks.items(), key=lambda kv: kv[0])
