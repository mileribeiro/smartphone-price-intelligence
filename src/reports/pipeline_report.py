from __future__ import annotations

import argparse
import json
from collections import Counter
from decimal import Decimal
from pathlib import Path
from statistics import median
from typing import Any

import duckdb

from src.quality.bronze_validation import validate_bronze


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _as_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _money(value: Decimal | float | int | None) -> str:
    if value is None:
        return "n/a"
    dec = Decimal(str(value)).quantize(Decimal("0.01"))
    return f"R$ {dec}"


def _pct(part: int, total: int) -> str:
    if total == 0:
        return "0.0%"
    return f"{part / total * 100:.1f}%"


def _top(counter: Counter[str], limit: int = 10) -> list[tuple[str, int]]:
    return [(key, value) for key, value in counter.most_common(limit) if key]


def _table_counts(db_path: Path) -> dict[str, int]:
    if not db_path.exists():
        return {}

    tables = [
        "bronze_product_listing_events",
        "raw_product_listing_events",
        "staging_product_listings",
        "staging_product_current",
        "dim_products",
        "dim_brands",
        "dim_sellers",
        "dim_conditions",
        "fact_product_prices",
        "mart_price_summary",
        "mart_free_shipping_by_condition",
        "mart_top_sellers_by_volume",
        "mart_discount_volume_correlation",
        "mart_avg_price_evolution",
        "mart_condition_distribution",
        "mart_price_variation_products",
        "mart_free_shipping_price_comparison",
        "mart_price_bands",
        "mart_seller_price_volume_balance",
        "mart_top_reviewed_products",
    ]
    conn = duckdb.connect(str(db_path))
    counts: dict[str, int] = {}
    try:
        for table in tables:
            try:
                counts[table] = conn.execute(f"select count(*) from {table}").fetchone()[0]
            except duckdb.Error:
                continue
    finally:
        conn.close()
    return counts


def build_pipeline_report(*, bronze_path: Path, db_path: Path, audit_path: Path, min_rows: int = 100) -> dict[str, Any]:
    rows = _load_jsonl(bronze_path)
    validation = validate_bronze(bronze_path=bronze_path, min_rows=min_rows)
    audit_rows = _load_jsonl(audit_path)

    prices = [price for price in (_as_decimal(row.get("price")) for row in rows) if price is not None]
    discounts = [discount for discount in (_as_decimal(row.get("discount_pct")) for row in rows) if discount is not None]
    product_ids = {row.get("item_id") for row in rows if row.get("item_id")}
    brands = Counter(str(row.get("brand")) for row in rows if row.get("brand"))
    sellers = Counter(str(row.get("seller_nickname")) for row in rows if row.get("seller_nickname"))
    queries = Counter(str(row.get("search_query")) for row in rows if row.get("search_query"))

    price_summary = {
        "min": min(prices) if prices else None,
        "avg": sum(prices) / len(prices) if prices else None,
        "median": median(prices) if prices else None,
        "max": max(prices) if prices else None,
    }

    return {
        "source": "Magazine Luiza",
        "bronze_path": str(bronze_path),
        "db_path": str(db_path),
        "audit_path": str(audit_path),
        "total_rows": len(rows),
        "unique_products": len(product_ids),
        "unique_brands": len(brands),
        "unique_sellers": len(sellers),
        "price_summary": price_summary,
        "products_with_discount": len(discounts),
        "avg_discount_pct": sum(discounts) / len(discounts) if discounts else None,
        "top_brands": _top(brands),
        "top_sellers": _top(sellers),
        "queries": _top(queries, limit=20),
        "validation": validation,
        "table_counts": _table_counts(db_path),
        "latest_audit": audit_rows[-1] if audit_rows else None,
    }


def render_markdown(report: dict[str, Any]) -> str:
    validation = report["validation"]
    field_coverage = validation["field_coverage"]
    checks = validation["checks"]
    price_summary = report["price_summary"]

    lines = [
        "# Relatório: Pipeline de Preços Magalu",
        "",
        "## Resumo Executivo",
        f"- Fonte final: {report['source']}",
        f"- Arquivo bronze: `{report['bronze_path']}`",
        f"- Total de eventos no bronze: {report['total_rows']}",
        f"- Produtos únicos: {report['unique_products']}",
        f"- Marcas identificadas: {report['unique_brands']}",
        f"- Vendedores identificados: {report['unique_sellers']}",
        f"- Status da validação: {'aprovado' if validation['passed'] else 'reprovado'}",
        "",
        "## Preços",
        f"- Menor preço: {_money(price_summary['min'])}",
        f"- Preço médio: {_money(price_summary['avg'])}",
        f"- Preço mediano: {_money(price_summary['median'])}",
        f"- Maior preço: {_money(price_summary['max'])}",
        f"- Produtos com desconto identificado: {report['products_with_discount']}",
        f"- Desconto médio identificado: {report['avg_discount_pct']:.2f}%" if report["avg_discount_pct"] is not None else "- Desconto médio identificado: n/a",
        "",
        "## Cobertura Dos Campos",
        "| Campo | Registros preenchidos | Cobertura |",
        "|---|---:|---:|",
    ]

    total_rows = report["total_rows"]
    for field, count in field_coverage.items():
        lines.append(f"| `{field}` | {count} | {_pct(count, total_rows)} |")

    lines.extend(["", "## Checks De Qualidade", "| Check | Resultado |", "|---|---|"])
    for check, passed in checks.items():
        lines.append(f"| `{check}` | {'OK' if passed else 'Falhou'} |")

    lines.extend(["", "## Top Marcas", "| Marca | Eventos |", "|---|---:|"])
    for brand, count in report["top_brands"]:
        lines.append(f"| {brand} | {count} |")

    lines.extend(["", "## Top Vendedores", "| Vendedor | Eventos |", "|---|---:|"])
    for seller, count in report["top_sellers"]:
        lines.append(f"| {seller} | {count} |")

    lines.extend(["", "## Queries Coletadas", "| Query | Eventos |", "|---|---:|"])
    for query, count in report["queries"]:
        lines.append(f"| {query} | {count} |")

    lines.extend(["", "## Tabelas Do Pipeline", "| Tabela | Linhas |", "|---|---:|"])
    for table, count in report["table_counts"].items():
        lines.append(f"| `{table}` | {count} |")

    latest_audit = report.get("latest_audit")
    if latest_audit:
        lines.extend(
            [
                "",
                "## Última Execução Auditada",
                f"- `collection_run_id`: `{latest_audit.get('collection_run_id')}`",
                f"- Status: `{latest_audit.get('status')}`",
                f"- Início: `{latest_audit.get('started_at')}`",
                f"- Fim: `{latest_audit.get('finished_at')}`",
                f"- Métricas: `{json.dumps(latest_audit.get('metrics', {}), ensure_ascii=False)}`",
            ]
        )

    lines.extend(
        [
            "",
            "## Limitações Conhecidas",
            "- O Magalu não expõe `sold_quantity` de forma consistente na busca pública; `review_count` foi mantido como proxy de tração.",
            "- Os marts de volume usam `sales_volume_proxy = coalesce(sold_quantity, review_count)`.",
            "- `free_shipping` depende do texto exibido no card; quando ausente, o campo fica nulo e é tratado nas análises.",
            "- A extração usa páginas públicas, por isso mudanças no HTML da fonte podem exigir ajustes no parser.",
        ]
    )

    return "\n".join(lines) + "\n"


def write_report(*, report: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_markdown(report), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Gera relatório executivo e técnico do pipeline.")
    parser.add_argument("--bronze", default="data/bronze/magalu_smartphones.jsonl", help="Arquivo bronze JSONL.")
    parser.add_argument("--db", default="data/warehouse/smartphone_price_intelligence.duckdb", help="Banco analítico DuckDB.")
    parser.add_argument("--audit", default="data/audit/collection_runs.jsonl", help="Arquivo JSONL de auditoria.")
    parser.add_argument("--output", default="reports/pipeline_execution_report.md", help="Relatório Markdown.")
    parser.add_argument("--min-rows", type=int, default=100, help="Volume mínimo esperado.")
    args = parser.parse_args()

    report = build_pipeline_report(
        bronze_path=Path(args.bronze),
        db_path=Path(args.db),
        audit_path=Path(args.audit),
        min_rows=args.min_rows,
    )
    write_report(report=report, output_path=Path(args.output))
    print(f"Relatório gerado em {args.output}")


if __name__ == "__main__":
    main()
