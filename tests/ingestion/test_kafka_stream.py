from __future__ import annotations

from src.ingestion.kafka_stream import publish_event_to_kafka


class _FakeFuture:
    def __init__(self) -> None:
        self.timeout = None

    def get(self, *, timeout: int) -> None:
        self.timeout = timeout


class _FakeProducer:
    def __init__(self) -> None:
        self.calls = []
        self.future = _FakeFuture()

    def send(self, topic, *, key, value):
        self.calls.append({"topic": topic, "key": key, "value": value})
        return self.future


def test_publish_event_to_kafka_uses_payload_hash_as_key():
    producer = _FakeProducer()
    event = {"payload_hash": "hash1", "source": "magalu", "item_id": "item1"}

    publish_event_to_kafka(producer=producer, topic="product-listing-events", event=event)

    assert producer.calls == [
        {
            "topic": "product-listing-events",
            "key": "hash1",
            "value": {
                "event_type": "product_listing_collected",
                "published_at": producer.calls[0]["value"]["published_at"],
                "payload_hash": "hash1",
                "payload": event,
            },
        }
    ]
    assert producer.future.timeout == 30

