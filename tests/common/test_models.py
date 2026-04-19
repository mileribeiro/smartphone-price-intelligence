from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.common.models import ProductListingEvent


def test_product_listing_event_coerces_decimal_bool_and_int_fields():
    event = ProductListingEvent(
        source="magalu",
        site_id="magazineluiza",
        category_id="smartphones",
        search_query="iphone",
        collection_run_id="RUN1",
        collected_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        collected_date="2025-12-31",
        item_id="ITEM1",
        title="iPhone",
        permalink=None,
        thumbnail=None,
        seller_id="123",
        seller_nickname="magazineluiza",
        price="1000.50",
        base_price=None,
        original_price=None,
        currency_id="BRL",
        discount_amount=None,
        discount_pct="10.5",
        pix_price="990.50",
        installment_count="10",
        installment_value="100.05",
        rating="4.8",
        review_count="1234",
        condition="new",
        listing_type_id=None,
        free_shipping="true",
        logistic_type=None,
        store_pick_up="false",
        sold_quantity=None,
        available_quantity=None,
        initial_quantity=None,
        brand="Apple",
        model="iPhone 13",
        line=None,
        storage_capacity="128 GB",
        ram_memory=None,
        color="Azul",
        catalog_product_id=None,
        domain_id=None,
        raw_payload={"x": 1},
        payload_hash="hash1",
    )

    assert isinstance(event.price, Decimal)
    assert event.price == Decimal("1000.50")
    assert event.discount_pct == Decimal("10.5")
    assert event.pix_price == Decimal("990.50")
    assert event.installment_count == 10
    assert event.installment_value == Decimal("100.05")
    assert event.rating == Decimal("4.8")
    assert event.review_count == 1234
    assert event.free_shipping is True
    assert event.store_pick_up is False
    assert event.seller_id == 123


def test_product_listing_event_requires_price_and_currency_id():
    with pytest.raises(Exception):
        ProductListingEvent(
            source="magalu",
            site_id="magazineluiza",
            category_id="smartphones",
            search_query="iphone",
            collection_run_id="RUN1",
            collected_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            collected_date="2025-12-31",
            item_id="ITEM1",
            title="iPhone",
            permalink=None,
            thumbnail=None,
            seller_id=None,
            seller_nickname=None,
            base_price=None,
            original_price=None,
            currency_id=None,  # type: ignore[arg-type]
            discount_amount=None,
            discount_pct=None,
            condition=None,
            listing_type_id=None,
            free_shipping=None,
            logistic_type=None,
            store_pick_up=None,
            sold_quantity=None,
            available_quantity=None,
            initial_quantity=None,
            brand=None,
            model=None,
            line=None,
            storage_capacity=None,
            ram_memory=None,
            color=None,
            catalog_product_id=None,
            domain_id=None,
            raw_payload={},
            payload_hash="hash1",
        )
