from __future__ import annotations

import json

from src.quality.bronze_validation import validate_bronze


def test_validate_bronze_passes_with_required_fields_and_review_proxy(tmp_path):
    bronze = tmp_path / "bronze.jsonl"
    rows = [
        {
            "source": "magalu",
            "item_id": "1",
            "title": "Smartphone",
            "price": "1000.00",
            "currency_id": "BRL",
            "condition": "new",
            "review_count": 10,
            "brand": "Samsung",
            "collected_at": "2026-04-16T00:00:00+00:00",
            "payload_hash": "hash1",
        }
    ]
    bronze.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    result = validate_bronze(bronze_path=bronze, min_rows=1)

    assert result["passed"] is True
    assert result["field_coverage"]["review_count"] == 1
