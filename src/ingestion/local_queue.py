from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _load_hashes(path: Path) -> set[str]:
    if not path.exists():
        return set()

    hashes: set[str] = set()
    for obj in _iter_jsonl(path):
        payload_hash = obj.get("payload_hash")
        if isinstance(payload_hash, str):
            hashes.add(payload_hash)
    return hashes


def publish_bronze_to_queue(*, bronze_path: Path, queue_path: Path) -> dict[str, Any]:
    queue_path.parent.mkdir(parents=True, exist_ok=True)

    existing_hashes = _load_hashes(queue_path)
    published = 0
    skipped_existing = 0
    invalid = 0

    with queue_path.open("a", encoding="utf-8") as out_f:
        for event in _iter_jsonl(bronze_path):
            payload_hash = event.get("payload_hash")
            if not isinstance(payload_hash, str):
                invalid += 1
                continue
            if payload_hash in existing_hashes:
                skipped_existing += 1
                continue

            envelope = {
                "event_type": "product_listing_collected",
                "published_at": datetime.now(timezone.utc).isoformat(),
                "payload_hash": payload_hash,
                "payload": event,
            }
            out_f.write(json.dumps(envelope, ensure_ascii=False, default=str) + "\n")
            existing_hashes.add(payload_hash)
            published += 1

    return {
        "published": published,
        "skipped_existing": skipped_existing,
        "invalid": invalid,
        "queue_path": str(queue_path),
    }

