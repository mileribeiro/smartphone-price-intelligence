from __future__ import annotations

import argparse
from pathlib import Path

from src.ingestion.local_queue import publish_bronze_to_queue
from src.warehouse.duckdb_store import consume_queue_to_raw, run_transformations


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pipeline local bronze -> queue -> DuckDB -> marts.")
    parser.add_argument("--bronze", required=True, help="Arquivo JSONL bronze de entrada.")
    parser.add_argument("--queue", default="data/queue/product_listing_events.jsonl", help="Fila local JSONL.")
    parser.add_argument("--db", default="data/warehouse/smartphone_price_intelligence.duckdb", help="Banco analítico DuckDB de saída.")
    return parser


def run_pipeline(
    *,
    bronze_path: Path,
    queue_path: Path,
    db_path: Path,
    publish_result: dict | None = None,
) -> dict:
    publish_result = publish_result or publish_bronze_to_queue(
        bronze_path=bronze_path,
        queue_path=queue_path,
    )
    consume_result = consume_queue_to_raw(
        queue_path=queue_path,
        db_path=db_path,
    )
    transform_result = run_transformations(db_path=db_path)
    return {
        "publish": publish_result,
        "consume": consume_result,
        "transform": transform_result,
    }


def main() -> None:
    args = build_arg_parser().parse_args()
    result = run_pipeline(
        bronze_path=Path(args.bronze),
        queue_path=Path(args.queue),
        db_path=Path(args.db),
    )

    print("PUBLISH", result["publish"])
    print("CONSUME", result["consume"])
    print("TRANSFORM", result["transform"])


if __name__ == "__main__":
    main()
