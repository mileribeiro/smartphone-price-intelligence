from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any

from src.orchestration.tasks import PipelineSettings, run_full_pipeline


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Orquestra o pipeline completo de monitoramento de preços.")
    parser.add_argument("--queries", default=_env("QUERIES", "smartphone,iphone,samsung galaxy,motorola,xiaomi,celular 5g,galaxy,redmi"))
    parser.add_argument("--pages", type=int, default=int(_env("PAGES", "2")))
    parser.add_argument("--limit", type=int, default=int(_env("LIMIT", "40")))
    parser.add_argument("--bronze", default=_env("BRONZE_PATH", "data/bronze/magalu_smartphones.jsonl"))
    parser.add_argument("--queue", default=_env("QUEUE_PATH", "data/queue/product_listing_events.jsonl"))
    parser.add_argument("--db", default=_env("DB_PATH", "data/warehouse/smartphone_price_intelligence.duckdb"))
    parser.add_argument("--audit", default=_env("AUDIT_PATH", "data/audit/collection_runs.jsonl"))
    parser.add_argument("--report", default=_env("REPORT_PATH", "reports/pipeline_execution_report.md"))
    parser.add_argument("--min-rows", type=int, default=int(_env("MIN_ROWS", "100")))
    parser.add_argument("--timeout", type=float, default=float(_env("REQUEST_TIMEOUT", "20")))
    parser.add_argument("--log-level", default=_env("LOG_LEVEL", "INFO"))
    parser.add_argument("--stream-backend", choices=["kafka", "local-jsonl"], default=_env("STREAM_BACKEND", "kafka"))
    parser.add_argument("--kafka-bootstrap-servers", default=_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"))
    parser.add_argument("--kafka-topic", default=_env("KAFKA_TOPIC", "product-listing-events"))
    parser.add_argument("--kafka-consumer-group", default=_env("KAFKA_CONSUMER_GROUP", "smartphone-price-intelligence-duckdb-loader"))
    parser.add_argument("--kafka-idle-timeout-seconds", type=float, default=float(_env("KAFKA_IDLE_TIMEOUT_SECONDS", "5")))
    parser.add_argument("--scheduled", action="store_true", help="Mantém o processo rodando em intervalo fixo.")
    parser.add_argument("--interval-hours", type=float, default=float(_env("INTERVAL_HOURS", "24")))
    return parser


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    return run_full_pipeline(PipelineSettings.from_args(args))


def main() -> None:
    args = build_arg_parser().parse_args()
    if not args.scheduled:
        print(json.dumps(run_once(args), ensure_ascii=False, default=str, indent=2))
        return

    while True:
        print(json.dumps(run_once(args), ensure_ascii=False, default=str, indent=2))
        time.sleep(max(args.interval_hours, 0.01) * 3600)


if __name__ == "__main__":
    main()
