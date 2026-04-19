from __future__ import annotations

from decimal import Decimal

from src.magalu.parser import extract_product_cards_from_html, parse_product_card


def test_parse_product_card_with_pix_discount_rating_and_shipping():
    text = (
        'Full Smartphone Samsung Galaxy A56 128GB 5G 8GB RAM Preto 6,7" '
        "Câm Tripla + Selfie 12MP 4.9 (3685) R$ 2.999,99 "
        "10x de R$ 199,89 sem juros ou R$ 1.799,00 no Pix "
        "( 10% de desconto no pix ) Frete grátis"
    )

    product = parse_product_card(
        href="/smartphone-samsung-galaxy-a56/p/abc123/te/tega/?seller_id=magazineluiza",
        text=text,
    )

    assert product is not None
    assert product.item_id == "abc123"
    assert product.brand == "Samsung"
    assert product.storage_capacity == "128 GB"
    assert product.ram_memory == "8 GB"
    assert product.price == Decimal("1799.00")
    assert product.original_price == Decimal("2999.99")
    assert product.pix_price == Decimal("1799.00")
    assert product.discount_pct == Decimal("10")
    assert product.installment_count == 10
    assert product.installment_value == Decimal("199.89")
    assert product.rating == Decimal("4.9")
    assert product.review_count == 3685
    assert product.free_shipping is True
    assert product.seller_name == "magazineluiza"


def test_extract_product_cards_from_html_deduplicates_products():
    html = """
    <html>
      <body>
        <a href="/smartphone-xiaomi/p/sku1/te/tega/">
          Smartphone Xiaomi Redmi 256GB 8GB RAM 4.7 (359)
          R$ 2.399,99 ou R$ 1.358,99 no Pix
        </a>
        <a href="/smartphone-xiaomi/p/sku1/te/tega/">
          Smartphone Xiaomi Redmi 256GB 8GB RAM 4.7 (359)
          R$ 2.399,99 ou R$ 1.358,99 no Pix
        </a>
      </body>
    </html>
    """

    products = extract_product_cards_from_html(html)

    assert len(products) == 1
    assert products[0].item_id == "sku1"
    assert products[0].brand == "Xiaomi"
