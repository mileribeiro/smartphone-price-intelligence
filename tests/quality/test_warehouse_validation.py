from __future__ import annotations

import json

from src.quality.warehouse_validation import validate_warehouse
from src.warehouse.duckdb_store import consume_queue_to_raw, run_transformations


def test_validate_warehouse_passes_with_materialized_layers(tmp_path):
    queue = tmp_path / "queue.jsonl"
    db = tmp_path / "warehouse.duckdb"
    payload = {
        "payload_hash": "hash1",
        "source": "magalu",
        "site_id": "magazineluiza",
        "category_id": "smartphones",
        "search_query": "smartphone",
        "collection_run_id": "run1",
        "collected_at": "2026-04-17T00:00:00+00:00",
        "collected_date": "2026-04-17",
        "item_id": "item1",
        "title": "Smartphone Samsung 128GB",
        "permalink": "https://example.com/item1",
        "seller_nickname": "magazineluiza",
        "price": "1000.00",
        "discount_pct": "10",
        "review_count": 100,
        "condition": "new",
        "free_shipping": True,
        "brand": "Samsung",
    }
    queue.write_text(
        json.dumps(
            {
                "event_type": "product_listing_collected",
                "published_at": "2026-04-17T00:01:00+00:00",
                "payload_hash": "hash1",
                "payload": payload,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    consume_queue_to_raw(queue_path=queue, db_path=db)
    run_transformations(db_path=db)

    result = validate_warehouse(db_path=db)

    assert result["passed"] is True
    assert result["checks"]["no_duplicate_payload_hash"] is True
    assert result["checks"]["price_and_discount_ranges_valid"] is True


def test_validate_warehouse_fails_when_required_tables_are_missing(tmp_path):
    db = tmp_path / "empty.duckdb"

    result = validate_warehouse(db_path=db)

    assert result["passed"] is False
    assert result["checks"]["all_required_tables_exist"] is False
    assert "fact_product_prices" in result["metrics"]["missing_tables"]
