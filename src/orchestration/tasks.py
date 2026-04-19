from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.audit.collection_runs import append_collection_run_audit
from src.common.time import utc_now
from src.ingestion.kafka_stream import consume_kafka_to_raw, create_kafka_producer, publish_event_to_kafka
from src.ingestion.local_queue import publish_bronze_to_queue
from src.magalu.service import MagaluCollectorService
from src.quality.bronze_validation import validate_bronze as validate_bronze_file
from src.quality.warehouse_validation import validate_warehouse
from src.reports.pipeline_report import build_pipeline_report, write_report
from src.warehouse.duckdb_store import (
    connect,
    create_bronze_table,
    table_counts,
    transform_bronze_to_staging,
    transform_staging_to_marts,
)
from src.warehouse.duckdb_store import consume_queue_to_raw


def _parse_queries(raw: str) -> list[str]:
    return [q.strip() for q in raw.split(",") if q.strip()]


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class PipelineSettings:
    queries: list[str]
    pages: int
    limit: int
    bronze_path: Path
    queue_path: Path
    db_path: Path
    audit_path: Path
    report_path: Path
    min_rows: int
    timeout_seconds: float
    log_level: str
    stream_backend: str
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_consumer_group: str
    kafka_idle_timeout_seconds: float

    @classmethod
    def from_env(cls) -> "PipelineSettings":
        return cls(
            queries=_parse_queries(_env("QUERIES", "smartphone,iphone,samsung galaxy,motorola,xiaomi,celular 5g,galaxy,redmi")),
            pages=int(_env("PAGES", "2")),
            limit=int(_env("LIMIT", "40")),
            bronze_path=Path(_env("BRONZE_PATH", "data/bronze/magalu_smartphones.jsonl")),
            queue_path=Path(_env("QUEUE_PATH", "data/queue/product_listing_events.jsonl")),
            db_path=Path(_env("DB_PATH", "data/warehouse/gocase.duckdb")),
            audit_path=Path(_env("AUDIT_PATH", "data/audit/collection_runs.jsonl")),
            report_path=Path(_env("REPORT_PATH", "reports/pipeline_execution_report.md")),
            min_rows=int(_env("MIN_ROWS", "100")),
            timeout_seconds=float(_env("REQUEST_TIMEOUT", "20")),
            log_level=_env("LOG_LEVEL", "INFO"),
            stream_backend=_env("STREAM_BACKEND", "kafka"),
            kafka_bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"),
            kafka_topic=_env("KAFKA_TOPIC", "product-listing-events"),
            kafka_consumer_group=_env("KAFKA_CONSUMER_GROUP", "gocase-duckdb-loader"),
            kafka_idle_timeout_seconds=float(_env("KAFKA_IDLE_TIMEOUT_SECONDS", "5")),
        )

    @classmethod
    def from_args(cls, args: Any) -> "PipelineSettings":
        return cls(
            queries=_parse_queries(args.queries),
            pages=args.pages,
            limit=args.limit,
            bronze_path=Path(args.bronze),
            queue_path=Path(args.queue),
            db_path=Path(args.db),
            audit_path=Path(args.audit),
            report_path=Path(args.report),
            min_rows=args.min_rows,
            timeout_seconds=args.timeout,
            log_level=args.log_level,
            stream_backend=args.stream_backend,
            kafka_bootstrap_servers=args.kafka_bootstrap_servers,
            kafka_topic=args.kafka_topic,
            kafka_consumer_group=args.kafka_consumer_group,
            kafka_idle_timeout_seconds=args.kafka_idle_timeout_seconds,
        )


def collect_and_publish(settings: PipelineSettings) -> dict[str, Any]:
    if not settings.queries:
        raise ValueError("Nenhuma query válida informada.")

    started_at = utc_now().isoformat()
    parameters = {
        "queries": settings.queries,
        "pages": settings.pages,
        "limit": settings.limit,
        "output": str(settings.bronze_path),
        "timeout_seconds": settings.timeout_seconds,
    }

    service = MagaluCollectorService()
    producer = None
    event_publisher = None
    if settings.stream_backend == "kafka":
        producer = create_kafka_producer(bootstrap_servers=settings.kafka_bootstrap_servers)

        def event_publisher(event):
            publish_event_to_kafka(
                producer=producer,
                topic=settings.kafka_topic,
                event=event.model_dump(),
            )

    try:
        collect_result = service.collect(
            queries=settings.queries,
            pages=settings.pages,
            limit=settings.limit,
            output_path=settings.bronze_path,
            timeout_seconds=settings.timeout_seconds,
            log_level=settings.log_level,
            event_publisher=event_publisher,
        )
        if producer is not None:
            producer.flush(timeout=30)
            producer.close(timeout=30)
    except Exception as e:
        if producer is not None:
            producer.close(timeout=5)
        append_collection_run_audit(
            audit_path=settings.audit_path,
            collection_run_id=None,
            source="magalu",
            status="failed",
            started_at=started_at,
            finished_at=utc_now().isoformat(),
            parameters=parameters,
            error=str(e),
        )
        raise

    append_collection_run_audit(
        audit_path=settings.audit_path,
        collection_run_id=collect_result.get("collection_run_id"),
        source="magalu",
        status="success",
        started_at=started_at,
        finished_at=utc_now().isoformat(),
        parameters=parameters,
        metrics={
            "total_pages": collect_result.get("total_pages"),
            "failed_pages": collect_result.get("failed_pages"),
            "total_cards": collect_result.get("total_cards"),
            "total_invalid_events": collect_result.get("total_invalid_events"),
            "total_valid_events_written": collect_result.get("total_valid_events_written"),
            "total_skipped_existing_payload_hash": collect_result.get("total_skipped_existing_payload_hash"),
            "total_events_published": collect_result.get("total_events_published"),
        },
    )
    return collect_result


def validate_bronze(settings: PipelineSettings) -> dict[str, Any]:
    result = validate_bronze_file(bronze_path=settings.bronze_path, min_rows=settings.min_rows)
    if not result["passed"]:
        raise RuntimeError(f"Validação bronze falhou: {json.dumps(result, ensure_ascii=False)}")
    return result


def consume_stream_to_bronze(settings: PipelineSettings) -> dict[str, Any]:
    if settings.stream_backend == "kafka":
        return consume_kafka_to_raw(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_topic,
            group_id=settings.kafka_consumer_group,
            db_path=settings.db_path,
            idle_timeout_seconds=settings.kafka_idle_timeout_seconds,
        )

    publish_result = publish_bronze_to_queue(
        bronze_path=settings.bronze_path,
        queue_path=settings.queue_path,
    )
    consume_result = consume_queue_to_raw(
        queue_path=settings.queue_path,
        db_path=settings.db_path,
    )
    return {"publish": publish_result, "consume": consume_result}


def build_staging(settings: PipelineSettings) -> dict[str, Any]:
    conn = connect(settings.db_path)
    try:
        create_bronze_table(conn)
        transform_bronze_to_staging(conn)
        return {"db_path": str(settings.db_path), "table_counts": table_counts(conn)}
    finally:
        conn.close()


def build_marts(settings: PipelineSettings) -> dict[str, Any]:
    conn = connect(settings.db_path)
    try:
        transform_staging_to_marts(conn)
        return {"db_path": str(settings.db_path), "table_counts": table_counts(conn)}
    finally:
        conn.close()


def validate_final_warehouse(settings: PipelineSettings) -> dict[str, Any]:
    result = validate_warehouse(db_path=settings.db_path)
    if not result["passed"]:
        raise RuntimeError(f"Validação warehouse falhou: {json.dumps(result, ensure_ascii=False)}")
    return result


def generate_report(settings: PipelineSettings) -> dict[str, Any]:
    report = build_pipeline_report(
        bronze_path=settings.bronze_path,
        db_path=settings.db_path,
        audit_path=settings.audit_path,
        min_rows=settings.min_rows,
    )
    write_report(report=report, output_path=settings.report_path)
    return {"report_path": str(settings.report_path)}


def run_full_pipeline(settings: PipelineSettings) -> dict[str, Any]:
    collect_result = collect_and_publish(settings)
    bronze_validation = validate_bronze(settings)
    consume_result = consume_stream_to_bronze(settings)
    staging_result = build_staging(settings)
    marts_result = build_marts(settings)
    warehouse_validation = validate_final_warehouse(settings)
    report_result = generate_report(settings)
    return {
        "collect": collect_result,
        "validation": bronze_validation,
        "consume": consume_result,
        "staging": staging_result,
        "marts": marts_result,
        "warehouse_validation": warehouse_validation,
        "report": report_result,
    }
