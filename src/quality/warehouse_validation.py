from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb

from src.warehouse.duckdb_store import BRONZE_TABLE, connect


REQUIRED_TABLES = [
    BRONZE_TABLE,
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
]


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    return (
        conn.execute(
            """
            select count(*)
            from information_schema.tables
            where table_schema = current_schema()
              and table_name = ?
            """,
            (table_name,),
        ).fetchone()[0]
        > 0
    )


def validate_warehouse(*, db_path: Path) -> dict[str, Any]:
    conn = connect(db_path)
    try:
        existing_tables = {table: _table_exists(conn, table) for table in REQUIRED_TABLES}
        missing_tables = [table for table, exists in existing_tables.items() if not exists]

        metrics: dict[str, Any] = {"missing_tables": missing_tables}
        checks: dict[str, bool] = {"all_required_tables_exist": not missing_tables}
        if missing_tables:
            return {
                "db_path": str(db_path),
                "checks": checks,
                "metrics": metrics,
                "passed": False,
            }

        metrics["bronze_rows"] = conn.execute(f"select count(*) from {BRONZE_TABLE}").fetchone()[0]
        metrics["staging_rows"] = conn.execute("select count(*) from staging_product_listings").fetchone()[0]
        metrics["fact_rows"] = conn.execute("select count(*) from fact_product_prices").fetchone()[0]
        metrics["mart_rows"] = conn.execute("select count(*) from mart_price_summary").fetchone()[0]

        metrics["required_nulls"] = conn.execute(
            """
            select count(*)
            from fact_product_prices
            where payload_hash is null
               or item_id is null
               or collected_date is null
               or price is null
               or title is null
            """
        ).fetchone()[0]

        metrics["duplicate_payload_hashes"] = conn.execute(
            """
            select count(*)
            from (
                select payload_hash
                from fact_product_prices
                group by payload_hash
                having count(*) > 1
            )
            """
        ).fetchone()[0]

        metrics["invalid_price_ranges"] = conn.execute(
            """
            select count(*)
            from fact_product_prices
            where price <= 0
               or price > 100000
               or (discount_pct is not null and (discount_pct < 0 or discount_pct > 100))
            """
        ).fetchone()[0]

        metrics["facts_without_product_dimension"] = conn.execute(
            """
            select count(*)
            from fact_product_prices f
            left join dim_products p using (item_id)
            where p.item_id is null
            """
        ).fetchone()[0]

        checks.update(
            {
                "bronze_has_rows": metrics["bronze_rows"] > 0,
                "staging_has_rows": metrics["staging_rows"] > 0,
                "fact_has_rows": metrics["fact_rows"] > 0,
                "marts_have_rows": metrics["mart_rows"] > 0,
                "required_fields_not_null": metrics["required_nulls"] == 0,
                "no_duplicate_payload_hash": metrics["duplicate_payload_hashes"] == 0,
                "price_and_discount_ranges_valid": metrics["invalid_price_ranges"] == 0,
                "facts_match_product_dimension": metrics["facts_without_product_dimension"] == 0,
            }
        )

        return {
            "db_path": str(db_path),
            "checks": checks,
            "metrics": metrics,
            "passed": all(checks.values()),
        }
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Valida as tabelas finais do warehouse DuckDB.")
    parser.add_argument("--db", default="data/warehouse/gocase.duckdb", help="Banco analítico DuckDB.")
    args = parser.parse_args()

    result = validate_warehouse(db_path=Path(args.db))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if not result["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
