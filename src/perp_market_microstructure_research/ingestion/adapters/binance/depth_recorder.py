import asyncio
import time
from contextlib import suppress
from pathlib import Path

import orjson
import websockets

from perp_market_microstructure_research.ingestion.writers.rotating_jsonl_writer import (
    WriteItem,
    writer_loop,
)

BINANCE_FSTREAM_WS = "wss://fstream.binance.com/ws"
WRITE_QUEUE_MAX = 500_000


def now_ns() -> int:
    return time.time_ns()


def minute_bucket(ts_ns: int) -> int:
    return int((ts_ns // 1_000_000_000) // 60)


def minute_filename(bucket: int) -> str:
    return f"deltas_utcmin_{bucket}.jsonl"


def validate_depth_msg(msg: dict) -> None:
    required = ["e", "E", "s", "U", "u", "b", "a"]
    for k in required:
        if k not in msg:
            raise ValueError(f"missing {k}")
    if msg["e"] != "depthUpdate":
        raise ValueError("not depthUpdate")


async def record_binance_depth(
    *,
    symbol: str,
    out_dir: Path,
    interval_ms: int = 100,
) -> None:
    stream = f"{symbol.lower()}@depth@{interval_ms}ms"
    url = f"{BINANCE_FSTREAM_WS}/{stream}"

    q = asyncio.Queue(maxsize=WRITE_QUEUE_MAX)
    writer_task = asyncio.create_task(
        writer_loop(out_dir=out_dir, filename_fn=minute_filename, queue=q)
    )

    conn_id = 0
    backoff = 0.5

    def ensure_writer_alive() -> None:
        if writer_task.done():
            exc = writer_task.exception()
            if exc:
                raise exc
            raise RuntimeError("writer task stopped unexpectedly")

    try:
        while True:
            ensure_writer_alive()
            try:
                async with websockets.connect(url, ping_interval=15) as ws:
                    conn_id += 1
                    backoff = 0.5
                    print(f"[binance] connected {stream}")

                    while True:
                        raw = await ws.recv()
                        recv_ts = now_ns()
                        msg = orjson.loads(raw)
                        validate_depth_msg(msg)

                        record = {
                            "exchange": "binance",
                            "symbol": symbol,
                            "conn_id": conn_id,
                            "recv_ts_ns": recv_ts,
                            "event_ts_ms": msg["E"],
                            "U": msg["U"],
                            "u": msg["u"],
                            "b": msg["b"],
                            "a": msg["a"],
                        }

                        bucket = minute_bucket(recv_ts)
                        await q.put(
                            WriteItem(bucket=bucket, line=orjson.dumps(record) + b"\n")
                        )
                        ensure_writer_alive()
            except Exception as e:
                print(f"[binance] error: {e}, reconnecting")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10.0)
    finally:
        if not writer_task.done():
            try:
                q.put_nowait(None)
            except asyncio.QueueFull:
                writer_task.cancel()
        with suppress(asyncio.CancelledError):
            await writer_task
