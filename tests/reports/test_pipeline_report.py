from __future__ import annotations

import json

from src.reports.pipeline_report import build_pipeline_report, render_markdown


def test_build_pipeline_report_summarizes_bronze_and_validation(tmp_path):
    bronze = tmp_path / "bronze.jsonl"
    db = tmp_path / "warehouse.db"
    audit = tmp_path / "audit.jsonl"

    row = {
        "source": "magalu",
        "item_id": "item1",
        "title": "Smartphone Samsung",
        "price": "1000.00",
        "currency_id": "BRL",
        "condition": "new",
        "review_count": 10,
        "brand": "Samsung",
        "seller_nickname": "magazineluiza",
        "collected_at": "2026-04-17T00:00:00+00:00",
        "payload_hash": "hash1",
    }
    bronze.write_text(json.dumps(row) + "\n", encoding="utf-8")
    audit.parent.mkdir(parents=True, exist_ok=True)
    audit.write_text(
        json.dumps({"collection_run_id": "run1", "status": "success", "metrics": {"total": 1}}) + "\n",
        encoding="utf-8",
    )

    report = build_pipeline_report(bronze_path=bronze, db_path=db, audit_path=audit, min_rows=1)
    markdown = render_markdown(report)

    assert report["total_rows"] == 1
    assert report["unique_products"] == 1
    assert report["validation"]["passed"] is True
    assert "Relatório: Pipeline de Preços Magalu" in markdown
