from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _coverage(rows: list[dict[str, Any]], field: str) -> int:
    return sum(1 for row in rows if row.get(field) is not None)


def validate_bronze(*, bronze_path: Path, min_rows: int = 100) -> dict[str, Any]:
    rows = _load_jsonl(bronze_path)
    payload_hashes = [row.get("payload_hash") for row in rows]
    duplicate_hashes = [item for item, count in Counter(payload_hashes).items() if item and count > 1]

    checks = {
        "has_minimum_rows": len(rows) >= min_rows,
        "all_rows_from_magalu": all(row.get("source") == "magalu" for row in rows),
        "all_have_item_id": _coverage(rows, "item_id") == len(rows),
        "all_have_title": _coverage(rows, "title") == len(rows),
        "all_have_price": _coverage(rows, "price") == len(rows),
        "all_have_currency": _coverage(rows, "currency_id") == len(rows),
        "all_have_collected_at": _coverage(rows, "collected_at") == len(rows),
        "all_have_condition": _coverage(rows, "condition") == len(rows),
        "all_have_payload_hash": _coverage(rows, "payload_hash") == len(rows),
        "no_duplicate_payload_hash": len(duplicate_hashes) == 0,
        "has_brand_coverage": _coverage(rows, "brand") > 0,
        "has_review_count_proxy": _coverage(rows, "review_count") > 0,
    }

    field_coverage = {
        field: _coverage(rows, field)
        for field in [
            "item_id",
            "title",
            "price",
            "original_price",
            "pix_price",
            "discount_pct",
            "seller_nickname",
            "free_shipping",
            "condition",
            "sold_quantity",
            "review_count",
            "collected_at",
            "brand",
            "storage_capacity",
            "ram_memory",
            "payload_hash",
        ]
    }

    return {
        "bronze_path": str(bronze_path),
        "total_rows": len(rows),
        "min_rows": min_rows,
        "checks": checks,
        "field_coverage": field_coverage,
        "notes": [
            "Magalu nao expoe sold_quantity de forma consistente na busca publica; review_count foi mantido como proxy de tracao.",
            "free_shipping depende do texto exibido no card da busca; quando ausente fica nulo e e tratado nas analises.",
            "seller_nickname e extraido do parametro seller_id da URL quando disponivel.",
        ],
        "passed": all(checks.values()),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Validação da camada Bronze JSONL.")
    parser.add_argument("--bronze", required=True, help="Arquivo bronze JSONL.")
    parser.add_argument("--min-rows", type=int, default=100, help="Volume mínimo esperado.")
    args = parser.parse_args()

    result = validate_bronze(bronze_path=Path(args.bronze), min_rows=args.min_rows)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if not result["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
