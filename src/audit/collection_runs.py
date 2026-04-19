from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def append_collection_run_audit(
    *,
    audit_path: Path,
    collection_run_id: str | None,
    source: str,
    status: str,
    started_at: str,
    finished_at: str,
    parameters: dict[str, Any],
    metrics: dict[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    audit_path.parent.mkdir(parents=True, exist_ok=True)

    row = {
        "collection_run_id": collection_run_id,
        "source": source,
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "parameters": parameters,
        "metrics": metrics or {},
        "error": error,
    }

    with audit_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")

    return row

