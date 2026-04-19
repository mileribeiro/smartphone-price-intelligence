from __future__ import annotations

import json

from src.warehouse.duckdb_store import consume_queue_to_raw, connect, run_transformations


def _payload(**overrides):
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
        "original_price": "1200.00",
        "pix_price": "990.00",
        "discount_amount": "200.00",
        "discount_pct": "10",
        "installment_count": 10,
        "installment_value": "100.00",
        "rating": "4.8",
        "review_count": 100,
        "sold_quantity": None,
        "condition": "new",
        "free_shipping": True,
        "brand": "Samsung",
        "storage_capacity": "128 GB",
        "ram_memory": "8 GB",
    }
    payload.update(overrides)
    return payload


def test_duckdb_store_consumes_queue_and_materializes_layers(tmp_path):
    queue = tmp_path / "queue.jsonl"
    db = tmp_path / "warehouse.duckdb"
    payloads = [
        _payload(),
        _payload(
            payload_hash="hash2",
            collected_at="2026-04-18T00:00:00+00:00",
            collected_date="2026-04-18",
            price="900.00",
            review_count=120,
        ),
        _payload(
            payload_hash="hash3",
            item_id="item2",
            title="Smartphone Motorola 256GB",
            seller_nickname="seller-two",
            price="1800.00",
            discount_pct="20",
            review_count=50,
            sold_quantity=80,
            free_shipping=False,
            brand="Motorola",
        ),
    ]
    queue.write_text(
        "\n".join(
            json.dumps(
                {
                    "event_type": "product_listing_collected",
                    "published_at": "2026-04-17T00:01:00+00:00",
                    "payload_hash": payload["payload_hash"],
                    "payload": payload,
                }
            )
            for payload in payloads
        )
        + "\n",
        encoding="utf-8",
    )

    consume_result = consume_queue_to_raw(queue_path=queue, db_path=db)
    transform_result = run_transformations(db_path=db)

    assert consume_result["inserted"] == 3
    assert transform_result["table_counts"]["bronze_product_listing_events"] == 3
    assert transform_result["table_counts"]["raw_product_listing_events"] == 3
    assert transform_result["table_counts"]["staging_product_listings"] == 3
    assert transform_result["table_counts"]["staging_product_current"] == 2
    assert transform_result["table_counts"]["fact_product_prices"] == 3
    assert transform_result["table_counts"]["dim_products"] == 2
    assert transform_result["table_counts"]["dim_brands"] == 2
    assert transform_result["table_counts"]["dim_sellers"] == 2
    assert transform_result["table_counts"]["dim_conditions"] == 1
    assert transform_result["table_counts"]["mart_price_summary"] == 2
    assert transform_result["table_counts"]["mart_price_bands"] == 2

    conn = connect(db)
    try:
        top_seller = conn.execute(
            "select seller_nickname, total_sales_volume_proxy from mart_top_sellers_by_volume order by total_sales_volume_proxy desc limit 1"
        ).fetchone()
        variation = conn.execute(
            "select price_variation, absolute_price_variation from mart_price_variation_products where item_id = 'item1'"
        ).fetchone()
        histogram = conn.execute(
            "select price_band_start, product_count from mart_price_bands order by price_band_start"
        ).fetchall()
        correlation_rows = conn.execute("select observations from mart_discount_volume_correlation").fetchone()[0]
        constraints = conn.execute(
            """
            select table_name, constraint_type, count(*) as total
            from information_schema.table_constraints
            where table_name in (
                'dim_products',
                'dim_brands',
                'dim_sellers',
                'dim_conditions',
                'fact_product_prices'
            )
            group by table_name, constraint_type
            """
        ).fetchall()
    finally:
        conn.close()

    assert top_seller == ("magazineluiza", 120)
    assert variation == (-100, 100)
    assert histogram == [(500, 1), (1500, 1)]
    assert correlation_rows == 2
    assert ("dim_products", "PRIMARY KEY", 1) in constraints
    assert ("dim_brands", "PRIMARY KEY", 1) in constraints
    assert ("dim_sellers", "PRIMARY KEY", 1) in constraints
    assert ("dim_conditions", "PRIMARY KEY", 1) in constraints
    assert ("fact_product_prices", "PRIMARY KEY", 1) in constraints
    assert ("fact_product_prices", "FOREIGN KEY", 4) in constraints
