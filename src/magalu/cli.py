from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.audit.collection_runs import append_collection_run_audit  # noqa: E402
from src.common.time import utc_now  # noqa: E402
from src.magalu.client import MagaluClientError  # noqa: E402
from src.magalu.service import MagaluCollectorService  # noqa: E402


def _parse_queries(raw: str) -> list[str]:
    return [q.strip() for q in raw.split(",") if q.strip()]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Coletor de listagens de smartphones no Magazine Luiza.")
    parser.add_argument("--queries", required=True, help='Queries separadas por vírgula. Ex: "smartphone,iphone"')
    parser.add_argument("--pages", type=int, default=1, help="Quantidade de páginas por query.")
    parser.add_argument("--limit", type=int, default=30, help="Quantidade máxima de cards por página.")
    parser.add_argument("--output", required=True, help="Arquivo JSONL de saída.")
    parser.add_argument("--timeout", type=float, default=20.0, help="Timeout por request, em segundos.")
    parser.add_argument("--collection-run-id", default=None, help="ID do run. Se omitido, é derivado do input.")
    parser.add_argument(
        "--audit-output",
        default="data/audit/collection_runs.jsonl",
        help="Arquivo JSONL de auditoria da execução.",
    )
    parser.add_argument("--log-level", default="INFO", help="INFO|WARNING|DEBUG...")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    queries = _parse_queries(args.queries)
    if not queries:
        raise SystemExit("Nenhuma query válida informada.")

    service = MagaluCollectorService()
    started_at = utc_now().isoformat()
    parameters = {
        "queries": queries,
        "pages": args.pages,
        "limit": args.limit,
        "output": args.output,
        "timeout_seconds": args.timeout,
    }
    try:
        result = service.collect(
            queries=queries,
            pages=args.pages,
            limit=args.limit,
            output_path=Path(args.output),
            timeout_seconds=args.timeout,
            collection_run_id=args.collection_run_id,
            log_level=args.log_level,
        )
    except MagaluClientError as e:
        append_collection_run_audit(
            audit_path=Path(args.audit_output),
            collection_run_id=args.collection_run_id,
            source="magalu",
            status="failed",
            started_at=started_at,
            finished_at=utc_now().isoformat(),
            parameters=parameters,
            error=str(e),
        )
        raise SystemExit(f"ERRO: {e}") from e

    append_collection_run_audit(
        audit_path=Path(args.audit_output),
        collection_run_id=result.get("collection_run_id"),
        source="magalu",
        status="success",
        started_at=started_at,
        finished_at=utc_now().isoformat(),
        parameters=parameters,
        metrics={
            "total_pages": result.get("total_pages"),
            "failed_pages": result.get("failed_pages"),
            "total_cards": result.get("total_cards"),
            "total_invalid_events": result.get("total_invalid_events"),
            "total_valid_events_written": result.get("total_valid_events_written"),
            "total_skipped_existing_payload_hash": result.get("total_skipped_existing_payload_hash"),
        },
    )
    print("OK", result)


if __name__ == "__main__":
    main()
