from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ProductListingEvent(BaseModel):
    source: str
    site_id: str
    category_id: str
    search_query: str
    collection_run_id: str
    collected_at: datetime
    collected_date: str

    item_id: str
    title: str
    permalink: Optional[str] = None
    thumbnail: Optional[str] = None

    seller_id: Optional[int] = None
    seller_nickname: Optional[str] = None

    price: Decimal
    base_price: Optional[Decimal] = None
    original_price: Optional[Decimal] = None
    currency_id: str

    discount_amount: Optional[Decimal] = None
    discount_pct: Optional[Decimal] = None
    pix_price: Optional[Decimal] = None
    installment_count: Optional[int] = None
    installment_value: Optional[Decimal] = None
    rating: Optional[Decimal] = None
    review_count: Optional[int] = None

    condition: Optional[str] = None
    listing_type_id: Optional[str] = None
    free_shipping: Optional[bool] = None
    logistic_type: Optional[str] = None
    store_pick_up: Optional[bool] = None

    sold_quantity: Optional[int] = None
    available_quantity: Optional[int] = None
    initial_quantity: Optional[int] = None

    brand: Optional[str] = None
    model: Optional[str] = None
    line: Optional[str] = None
    storage_capacity: Optional[str] = None
    ram_memory: Optional[str] = None
    color: Optional[str] = None

    catalog_product_id: Optional[str] = None
    domain_id: Optional[str] = None

    raw_payload: dict[str, Any] = Field(default_factory=dict)
    payload_hash: str

    model_config = ConfigDict(extra="ignore")

    @field_validator(
        "price",
        "base_price",
        "original_price",
        "discount_amount",
        "discount_pct",
        "pix_price",
        "installment_value",
        "rating",
        mode="before",
    )
    @classmethod
    def _coerce_decimal(cls, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, Decimal):
            return v
        if isinstance(v, (int, float)):
            return Decimal(str(v))
        if isinstance(v, str):
            v = v.strip().replace(",", ".")
            if v == "":
                raise ValueError("Empty decimal string")
            return Decimal(v)
        return Decimal(v)

    @field_validator(
        "seller_id",
        "sold_quantity",
        "available_quantity",
        "initial_quantity",
        "installment_count",
        "review_count",
        mode="before",
    )
    @classmethod
    def _coerce_int(cls, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, bool):
            raise ValueError("Invalid int for quantity")
        if isinstance(v, int):
            return v
        if isinstance(v, float):
            return int(v)
        if isinstance(v, str):
            v = v.strip()
            if v == "":
                return None
            return int(float(v))
        return int(v)

    @field_validator(
        "free_shipping",
        "store_pick_up",
        mode="before",
    )
    @classmethod
    def _coerce_bool(cls, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            vv = v.strip().lower()
            if vv in {"true", "t", "1", "yes", "y"}:
                return True
            if vv in {"false", "f", "0", "no", "n"}:
                return False
        return bool(v)
