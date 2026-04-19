from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from decimal import Decimal
from html import unescape
from html.parser import HTMLParser
from typing import Optional
from urllib.parse import parse_qs, urljoin, urlparse


PRODUCT_URL_RE = re.compile(r"/p/([^/?#]+)")
MONEY_RE = re.compile(r"R\$\s*\d{1,3}(?:\.\d{3})*,\d{2}")
RATING_RE = re.compile(r"(?<!\d)([1-5][,.]\d)\s*\((\d+)\)")
INSTALLMENT_RE = re.compile(r"(\d+)x\s+de\s+(R\$\s*\d{1,3}(?:\.\d{3})*,\d{2})", re.IGNORECASE)
DISCOUNT_RE = re.compile(r"(\d+(?:[,.]\d+)?)%\s+de\s+desconto", re.IGNORECASE)
BRANDS = [
    "Apple",
    "Samsung",
    "Motorola",
    "Xiaomi",
    "Realme",
    "OPPO",
    "Infinix",
    "TCL",
    "Nokia",
    "Asus",
    "LG",
    "Multilaser",
    "Philco",
    "Positivo",
]


@dataclass(frozen=True)
class MagaluProductCard:
    item_id: str
    title: str
    permalink: str
    price: Decimal
    original_price: Optional[Decimal]
    pix_price: Optional[Decimal]
    discount_pct: Optional[Decimal]
    installment_count: Optional[int]
    installment_value: Optional[Decimal]
    rating: Optional[Decimal]
    review_count: Optional[int]
    free_shipping: Optional[bool]
    is_full: Optional[bool]
    seller_name: Optional[str]
    brand: Optional[str]
    storage_capacity: Optional[str]
    ram_memory: Optional[str]
    raw_text: str


class _AnchorCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._stack: list[dict] = []
        self.anchors: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        if tag.lower() != "a":
            return
        attrs_dict = dict(attrs)
        href = attrs_dict.get("href")
        if not href:
            return
        self._stack.append({"href": href, "parts": []})

    def handle_data(self, data: str) -> None:
        for anchor in self._stack:
            anchor["parts"].append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or not self._stack:
            return
        anchor = self._stack.pop()
        text = clean_text(" ".join(anchor["parts"]))
        self.anchors.append((anchor["href"], text))


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", unescape(value or "")).strip()


def parse_brl_money(value: str) -> Decimal:
    cleaned = value.replace("R$", "").strip().replace(".", "").replace(",", ".")
    return Decimal(cleaned)


def _extract_item_id(url: str, title: str) -> str:
    match = PRODUCT_URL_RE.search(url)
    if match:
        return match.group(1)
    digest = hashlib.sha256(f"{url}|{title}".encode("utf-8")).hexdigest()[:16]
    return f"magalu-{digest}"


def _extract_brand(title: str) -> str | None:
    normalized_title = title.lower()
    for brand in BRANDS:
        if brand.lower() in normalized_title:
            return brand
    if "iphone" in normalized_title:
        return "Apple"
    return None


def _extract_storage(title: str) -> str | None:
    match = re.search(r"\b(\d+)\s*(GB|TB)\b", title, flags=re.IGNORECASE)
    if not match:
        return None
    return f"{match.group(1)} {match.group(2).upper()}"


def _extract_ram(title: str) -> str | None:
    match = re.search(r"\b(\d+)\s*GB\s+RAM\b", title, flags=re.IGNORECASE)
    if not match:
        return None
    return f"{match.group(1)} GB"


def _extract_seller_name(permalink: str) -> str | None:
    seller_id = parse_qs(urlparse(permalink).query).get("seller_id")
    if seller_id and seller_id[0]:
        return seller_id[0]
    return None


def _extract_title(text: str) -> str:
    title = MONEY_RE.split(text, maxsplit=1)[0]
    title = RATING_RE.sub("", title)
    title = re.sub(r"^(Full|Patrocinado)\s+", "", title, flags=re.IGNORECASE)
    title = re.sub(r"^(Full|Patrocinado)\s+", "", title, flags=re.IGNORECASE)
    return clean_text(title)


def parse_product_card(*, href: str, text: str, base_url: str = "https://www.magazineluiza.com.br") -> MagaluProductCard | None:
    text = clean_text(text)
    if "R$" not in text:
        return None
    if "/p/" not in href and "magazineluiza.com.br" not in href:
        return None

    permalink = urljoin(base_url, href)
    title = _extract_title(text)
    if not title:
        return None

    prices = [parse_brl_money(match.group(0)) for match in MONEY_RE.finditer(text)]
    if not prices:
        return None

    pix_price = None
    pix_match = re.search(r"(R\$\s*\d{1,3}(?:\.\d{3})*,\d{2})\s+no Pix", text, flags=re.IGNORECASE)
    if pix_match:
        pix_price = parse_brl_money(pix_match.group(1))

    price = pix_price or prices[-1]
    original_price = None
    if len(prices) > 1 and prices[0] > price:
        original_price = prices[0]

    discount_pct = None
    discount_match = DISCOUNT_RE.search(text)
    if discount_match:
        discount_pct = Decimal(discount_match.group(1).replace(",", "."))
    elif original_price and original_price > 0:
        discount_pct = (original_price - price) / original_price * Decimal("100")

    installment_count = None
    installment_value = None
    installment_match = INSTALLMENT_RE.search(text)
    if installment_match:
        installment_count = int(installment_match.group(1))
        installment_value = parse_brl_money(installment_match.group(2))

    rating = None
    review_count = None
    rating_match = RATING_RE.search(text)
    if rating_match:
        rating = Decimal(rating_match.group(1).replace(",", "."))
        review_count = int(rating_match.group(2))

    return MagaluProductCard(
        item_id=_extract_item_id(permalink, title),
        title=title,
        permalink=permalink,
        price=price,
        original_price=original_price,
        pix_price=pix_price,
        discount_pct=discount_pct,
        installment_count=installment_count,
        installment_value=installment_value,
        rating=rating,
        review_count=review_count,
        free_shipping=True if re.search(r"frete grátis", text, flags=re.IGNORECASE) else None,
        is_full=True if re.search(r"\bFull\b", text, flags=re.IGNORECASE) else None,
        seller_name=_extract_seller_name(permalink),
        brand=_extract_brand(title),
        storage_capacity=_extract_storage(title),
        ram_memory=_extract_ram(title),
        raw_text=text,
    )


def extract_product_cards_from_html(html: str, *, limit: int | None = None) -> list[MagaluProductCard]:
    collector = _AnchorCollector()
    collector.feed(html)

    products: list[MagaluProductCard] = []
    seen: set[str] = set()
    for href, text in collector.anchors:
        product = parse_product_card(href=href, text=text)
        if product is None:
            continue
        if product.item_id in seen:
            continue
        seen.add(product.item_id)
        products.append(product)
        if limit is not None and len(products) >= limit:
            break
    return products
