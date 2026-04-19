from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.warehouse.duckdb_store import connect, create_bronze_table, insert_event_to_bronze


def _load_kafka_classes():
    from kafka import KafkaConsumer, KafkaProducer
    from kafka.errors import NoBrokersAvailable

    return KafkaConsumer, KafkaProducer, NoBrokersAvailable


def create_kafka_producer(*, bootstrap_servers: str, retries: int = 20, retry_sleep_seconds: float = 2.0):
    _, KafkaProducer, NoBrokersAvailable = _load_kafka_classes()
    last_error = None
    for _ in range(retries):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                key_serializer=lambda value: value.encode("utf-8"),
                value_serializer=lambda value: json.dumps(value, ensure_ascii=False, default=str).encode("utf-8"),
                acks="all",
                retries=5,
            )
        except NoBrokersAvailable as e:
            last_error = e
            time.sleep(retry_sleep_seconds)
    raise RuntimeError(f"Kafka indisponível em {bootstrap_servers}") from last_error


def publish_event_to_kafka(*, producer: Any, topic: str, event: dict[str, Any]) -> None:
    payload_hash = event["payload_hash"]
    envelope = {
        "event_type": "product_listing_collected",
        "published_at": datetime.now(timezone.utc).isoformat(),
        "payload_hash": payload_hash,
        "payload": event,
    }
    producer.send(topic, key=payload_hash, value=envelope).get(timeout=30)


def consume_kafka_to_raw(
    *,
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    db_path: Path,
    idle_timeout_seconds: float = 5.0,
    max_messages: int | None = None,
) -> dict[str, Any]:
    KafkaConsumer, _, _ = _load_kafka_classes()
    conn = connect(db_path)
    create_bronze_table(conn)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=int(idle_timeout_seconds * 1000),
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    inserted = 0
    skipped_existing = 0
    invalid = 0
    consumed = 0
    ingested_at = datetime.now(timezone.utc).isoformat()

    try:
        for message in consumer:
            consumed += 1
            if max_messages is not None and consumed > max_messages:
                break
            try:
                envelope = message.value
                payload = envelope["payload"]
                payload_hash = payload["payload_hash"]
            except Exception:
                invalid += 1
                continue

            if insert_event_to_bronze(conn, payload=payload, ingested_at=ingested_at):
                inserted += 1
            else:
                skipped_existing += 1

        consumer.commit()
    finally:
        consumer.close()
        conn.close()

    return {
        "consumed": consumed,
        "inserted": inserted,
        "skipped_existing": skipped_existing,
        "invalid": invalid,
        "topic": topic,
        "db_path": str(db_path),
    }
