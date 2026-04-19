from __future__ import annotations

import json

from src.ingestion.local_queue import publish_bronze_to_queue


def test_publish_bronze_to_queue_is_idempotent(tmp_path):
    bronze = tmp_path / "bronze.jsonl"
    queue = tmp_path / "queue.jsonl"
    event = {"payload_hash": "hash1", "source": "magalu", "item_id": "item1"}
    bronze.write_text(json.dumps(event) + "\n", encoding="utf-8")

    first = publish_bronze_to_queue(bronze_path=bronze, queue_path=queue)
    second = publish_bronze_to_queue(bronze_path=bronze, queue_path=queue)

    assert first["published"] == 1
    assert second["published"] == 0
    assert second["skipped_existing"] == 1
    assert len(queue.read_text(encoding="utf-8").splitlines()) == 1

