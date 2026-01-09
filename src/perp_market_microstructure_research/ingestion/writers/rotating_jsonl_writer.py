import asyncio
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

BATCH_SIZE = 2000
FLUSH_INTERVAL_SEC = 0.5


@dataclass
class WriteItem:
    bucket: int
    line: bytes


class RotatingJSONLWriter:
    def __init__(self, out_dir: Path, *, filename_fn):
        self.out_dir = out_dir
        self.filename_fn = filename_fn
        self.current_bucket: Optional[int] = None
        self.fp = None
        self.buffer = []
        self.last_flush = time.time()

    def _open(self, bucket: int):
        self.out_dir.mkdir(parents=True, exist_ok=True)
        name = self.filename_fn(bucket)
        self.tmp = self.out_dir / f"{name}.tmp"
        self.final = self.out_dir / name
        self.fp = self.tmp.open("ab", buffering=1024 * 1024)
        self.current_bucket = bucket
        self.last_flush = time.time()

    def _close(self):
        if self.fp:
            if self.buffer:
                self.fp.write(b"".join(self.buffer))
                self.buffer.clear()
            self.fp.flush()
            self.fp.close()
            self.tmp.replace(self.final)
            self.fp = None
            self.current_bucket = None

    def write(self, item: WriteItem):
        if self.current_bucket != item.bucket:
            self._close()
            self._open(item.bucket)

        self.buffer.append(item.line)
        now = time.time()
        if len(self.buffer) >= BATCH_SIZE or now - self.last_flush >= FLUSH_INTERVAL_SEC:
            self.fp.write(b"".join(self.buffer))
            self.fp.flush()
            self.buffer.clear()
            self.last_flush = now

    def close(self):
        self._close()


async def writer_loop(*, out_dir: Path, filename_fn, queue: asyncio.Queue):
    writer = RotatingJSONLWriter(out_dir, filename_fn=filename_fn)
    try:
        while True:
            item = await queue.get()
            if item is None:
                break
            writer.write(item)
    finally:
        writer.close()
