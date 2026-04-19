from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from src.common.hashing import payload_hash_from_event, sha256_hex, stable_json_dumps
from src.common.logging import get_logger
from src.common.models import ProductListingEvent
from src.common.time import business_date, utc_now
from src.magalu.client import MagaluClient, MagaluClientError
from src.magalu.parser import MagaluProductCard, extract_product_cards_from_html


class MagaluCollectorService:
    def __init__(
        self,
        *,
        client: Optional[MagaluClient] = None,
        source: str = "magalu",
        site_id: str = "magazineluiza",
        category_id: str = "smartphones",
    ) -> None:
        self._client = client or MagaluClient()
        self._source = source
        self._site_id = site_id
        self._category_id = category_id

    def _derive_collection_run_id(self, *, queries: list[str], pages: int, limit: int) -> str:
        payload = {
            "source": self._source,
            "site_id": self._site_id,
            "category_id": self._category_id,
            "queries": queries,
            "pages": pages,
            "limit": limit,
        }
        return sha256_hex(stable_json_dumps(payload))[:32]

    def _load_existing_payload_hashes(self, output_path: Path) -> set[str]:
        if not output_path.exists():
            return set()

        payload_hashes: set[str] = set()
        try:
            with output_path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    payload_hash = obj.get("payload_hash")
                    if isinstance(payload_hash, str):
                        payload_hashes.add(payload_hash)
        except OSError:
            return set()
        return payload_hashes

    def _event_fields_for_hash(self, event_fields: dict[str, Any]) -> dict[str, Any]:
        hash_fields = {
            k: v
            for k, v in event_fields.items()
            if k not in {"payload_hash", "collected_at", "collection_run_id"}
        }
        hash_fields["collected_date"] = event_fields.get("collected_date")
        return hash_fields

    def _event_fields_from_card(
        self,
        *,
        card: MagaluProductCard,
        search_query: str,
        collection_run_id: str,
        page_url: str,
    ) -> dict[str, Any]:
        discount_amount = None
        if card.original_price is not None:
            discount_amount = card.original_price - card.price

        collected_at = utc_now()

        return {
            "source": self._source,
            "site_id": self._site_id,
            "category_id": self._category_id,
            "search_query": search_query,
            "collection_run_id": collection_run_id,
            "collected_at": collected_at,
            "collected_date": business_date(collected_at),
            "item_id": card.item_id,
            "title": card.title,
            "permalink": card.permalink,
            "thumbnail": None,
            "seller_id": None,
            "seller_nickname": card.seller_name,
            "price": card.price,
            "base_price": None,
            "original_price": card.original_price,
            "currency_id": "BRL",
            "discount_amount": discount_amount,
            "discount_pct": card.discount_pct,
            "pix_price": card.pix_price,
            "installment_count": card.installment_count,
            "installment_value": card.installment_value,
            "rating": card.rating,
            "review_count": card.review_count,
            "condition": "new",
            "listing_type_id": "full" if card.is_full else None,
            "free_shipping": card.free_shipping,
            "logistic_type": "full" if card.is_full else None,
            "store_pick_up": None,
            "sold_quantity": None,
            "available_quantity": None,
            "initial_quantity": None,
            "brand": card.brand,
            "model": None,
            "line": None,
            "storage_capacity": card.storage_capacity,
            "ram_memory": card.ram_memory,
            "color": None,
            "catalog_product_id": None,
            "domain_id": "MAGALU-SMARTPHONES",
            "raw_payload": {
                "page_url": page_url,
                "card_text": card.raw_text,
            },
        }

    def collect(
        self,
        *,
        queries: list[str],
        pages: int,
        limit: int,
        output_path: Path,
        timeout_seconds: Optional[float] = None,
        collection_run_id: Optional[str] = None,
        log_level: str = "INFO",
        event_publisher: Optional[Callable[[ProductListingEvent], None]] = None,
    ) -> Dict[str, Any]:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        run_id = collection_run_id or self._derive_collection_run_id(queries=queries, pages=pages, limit=limit)
        logger = get_logger("magalu-collector", level=getattr(__import__("logging"), log_level, 20), run_id=run_id)

        if timeout_seconds is not None:
            self._client._timeout_seconds = float(timeout_seconds)  # type: ignore[attr-defined]

        existing_hashes = self._load_existing_payload_hashes(output_path)
        total_pages = 0
        failed_pages = 0
        total_cards = 0
        invalid_events = 0
        written = 0
        skipped_existing = 0
        published = 0

        with output_path.open("a", encoding="utf-8") as out_f:
            for query in queries:
                for page in range(1, pages + 1):
                    total_pages += 1
                    try:
                        search_page = self._client.search(query=query, page=page)
                    except MagaluClientError as e:
                        failed_pages += 1
                        logger.warning(f"Falha na busca Magalu (query={query!r}, page={page}): {e}")
                        continue

                    cards = extract_product_cards_from_html(search_page.html, limit=limit)
                    total_cards += len(cards)
                    logger.debug(f"query={query!r} page={page} cards={len(cards)} url={search_page.url}")

                    for card in cards:
                        try:
                            event_fields = self._event_fields_from_card(
                                card=card,
                                search_query=query,
                                collection_run_id=run_id,
                                page_url=search_page.url,
                            )
                            payload_hash = payload_hash_from_event(self._event_fields_for_hash(event_fields))
                            event_fields["payload_hash"] = payload_hash
                            event = ProductListingEvent(**event_fields)
                        except Exception as e:
                            invalid_events += 1
                            logger.debug(f"Evento inválido Magalu item_id={card.item_id}: {e}")
                            continue

                        if event_publisher is not None:
                            event_publisher(event)
                            published += 1

                        if payload_hash in existing_hashes:
                            skipped_existing += 1
                            continue

                        out_f.write(json.dumps(event.model_dump(), default=str, ensure_ascii=False) + "\n")
                        out_f.flush()
                        existing_hashes.add(payload_hash)
                        written += 1

        logger.info(f"total_pages={total_pages}")
        logger.info(f"failed_pages={failed_pages}")
        logger.info(f"total_cards={total_cards}")
        logger.info(f"total_invalid_events={invalid_events}")
        logger.info(f"total_valid_events_written={written}")
        logger.info(f"total_skipped_existing_payload_hash={skipped_existing}")
        logger.info(f"total_events_published={published}")

        if total_pages > 0 and failed_pages == total_pages:
            raise MagaluClientError("Todas as páginas de busca falharam; nenhum dado foi coletado.")

        return {
            "collection_run_id": run_id,
            "total_pages": total_pages,
            "failed_pages": failed_pages,
            "total_cards": total_cards,
            "total_invalid_events": invalid_events,
            "total_valid_events_written": written,
            "total_skipped_existing_payload_hash": skipped_existing,
            "total_events_published": published,
            "output_path": str(output_path),
        }
