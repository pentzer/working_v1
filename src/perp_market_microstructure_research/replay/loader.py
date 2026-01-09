from collections.abc import Iterator
from pathlib import Path

import orjson


def load_normalized_deltas(path: Path) -> Iterator[dict]:
    with path.open("rb") as fin:
        for line in fin:
            yield orjson.loads(line)
