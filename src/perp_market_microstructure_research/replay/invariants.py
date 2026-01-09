from pathlib import Path

from perp_market_microstructure_research.core.orderbook.snapshot import snapshot_l2
from perp_market_microstructure_research.replay.driver import replay_file


def assert_deterministic_replay(path: Path) -> None:
    first = snapshot_l2(replay_file(path))
    second = snapshot_l2(replay_file(path))
    if first != second:
        raise AssertionError("deterministic replay invariant failed")
