from collections.abc import Mapping

from perp_market_microstructure_research.core.orderbook.state import (
    OrderBookState,
    _side_map,
)


def _require_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an int")
    return value


def apply_l2_delta(state: OrderBookState, delta: Mapping[str, object]) -> None:
    if not isinstance(delta, Mapping):
        raise ValueError("delta must be a mapping")

    if "price" not in delta or "size" not in delta or "side" not in delta:
        raise ValueError("delta must include price, size, and side")

    price = _require_int("price", delta["price"])
    size = _require_int("size", delta["size"])
    side = delta["side"]

    if size < 0:
        raise ValueError("size must be >= 0")

    update_type = delta.get("update_type")
    if update_type is not None:
        if not isinstance(update_type, str):
            raise ValueError("update_type must be a string")
        action = update_type.lower()
        if action in ("delete", "remove"):
            if size != 0:
                raise ValueError("delete updates must have size == 0")
        elif action in ("set", "update", "insert"):
            if size == 0:
                raise ValueError("set/update inserts must have size > 0")
        else:
            raise ValueError(f"unknown update_type: {update_type!r}")

    book = _side_map(state, side)
    if size == 0:
        book.pop(price, None)
    else:
        book[price] = size
