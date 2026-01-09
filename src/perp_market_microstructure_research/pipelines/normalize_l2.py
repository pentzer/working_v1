import json
import time
from pathlib import Path
from typing import Optional

import orjson

from perp_market_microstructure_research.core.schemas.l2_delta import (
    is_depth_delta,
    normalize_delta,
)
from perp_market_microstructure_research.validation.audit import Audit
from perp_market_microstructure_research.validation.continuity import continuity_ok

TMP_RECOVERY_MIN_AGE_SEC = 300


def list_raw_files(raw_dir: Path):
    return sorted(raw_dir.glob("deltas_utcmin_*.jsonl"))


def recover_tmp_files(raw_dir: Path, *, min_age_sec: int = TMP_RECOVERY_MIN_AGE_SEC) -> None:
    now = time.time()
    for tmp in raw_dir.glob("deltas_utcmin_*.jsonl.tmp"):
        final = tmp.with_suffix("")
        if final.exists():
            continue
        age = now - tmp.stat().st_mtime
        if age >= min_age_sec:
            tmp.replace(final)


def process_raw_dir(raw_dir: Path):
    base = raw_dir.parent
    norm_dir = base / "normalized"
    audit_dir = base / "audit"

    norm_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)

    recover_tmp_files(raw_dir)

    for raw_file in list_raw_files(raw_dir):
        stem = raw_file.stem
        norm_file = norm_dir / f"{stem}.fp.jsonl"
        audit_file = audit_dir / f"{stem}.audit.json"

        if norm_file.exists() and audit_file.exists():
            continue

        audit = Audit()
        prev_u: Optional[int] = None

        with raw_file.open("rb") as fin, norm_file.open("wb") as fout:
            for line in fin:
                audit.raw_lines += 1
                try:
                    raw = orjson.loads(line)
                except Exception:
                    audit.bad_json += 1
                    continue

                if not is_depth_delta(raw):
                    audit.skipped_schema += 1
                    continue

                try:
                    d = normalize_delta(raw)
                except Exception:
                    audit.normalize_errors += 1
                    continue

                audit.kept_lines += 1

                if not continuity_ok(prev_u, d["U"], d["u"]):
                    audit.continuity_ok = False
                    audit.gaps += 1
                    if audit.first_gap is None:
                        audit.first_gap = {"prev_u": prev_u, "U": d["U"], "u": d["u"]}
                    prev_u = None
                else:
                    prev_u = d["u"]

                fout.write(orjson.dumps(d) + b"\n")

        with audit_file.open("w") as fa:
            json.dump(
                {
                    "raw_file": str(raw_file),
                    "normalized_file": str(norm_file),
                    "created_at_unix": int(time.time()),
                    "stats": audit.__dict__,
                },
                fa,
                indent=2,
            )


def main():
    process_raw_dir(Path("data/binance/BTCUSDT/raw"))
