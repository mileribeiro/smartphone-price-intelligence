from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import duckdb


class DashboardDataError(RuntimeError):
    """Erro ao ler os dados do dashboard."""


def _connect(db_path: Path | str) -> duckdb.DuckDBPyConnection:
    path = Path(db_path)
    if not path.exists():
        raise DashboardDataError(f"Arquivo DuckDB não encontrado: {path}")
    try:
        return duckdb.connect(str(path), read_only=True)
    except duckdb.Error as exc:
        raise DashboardDataError(str(exc)) from exc


def _fetch_rows(db_path: Path | str, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
    conn = _connect(db_path)
    try:
        result = conn.execute(sql, tuple(params))
        columns = [column[0] for column in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]
    except duckdb.Error as exc:
        raise DashboardDataError(str(exc)) from exc
    finally:
        conn.close()


def _fetch_one(db_path: Path | str, sql: str, params: Iterable[Any] = ()) -> dict[str, Any]:
    rows = _fetch_rows(db_path, sql, params)
    return rows[0] if rows else {}


def _table_columns(db_path: Path | str, table_name: str) -> set[str]:
    rows = _fetch_rows(
        db_path,
        """
        select column_name
        from information_schema.columns
        where table_schema = current_schema()
          and table_name = ?
        """,
        (table_name,),
    )
    return {row["column_name"] for row in rows}


def get_last_collection_info(db_path: Path | str) -> dict[str, Any]:
    return _fetch_one(
        db_path,
        """
        select
            count(*) as total_events,
            count(distinct item_id) as total_bronze_items,
            count(distinct collection_run_id) as total_collection_runs,
            max(collected_at) as last_collected_at
        from bronze_product_listing_events
        """,
    )


def get_price_kpis(db_path: Path | str) -> dict[str, Any]:
    return _fetch_one(
        db_path,
        """
        select
            count(distinct item_id) as product_count,
            avg(price) as avg_price,
            min(price) as min_price,
            max(price) as max_price
        from staging_product_current
        """,
    )


def get_free_shipping_by_condition(db_path: Path | str) -> list[dict[str, Any]]:
    return _fetch_rows(
        db_path,
        """
        select
            condition,
            product_count,
            free_shipping_count,
            paid_shipping_count,
            unknown_shipping_count,
            free_shipping_pct_known
        from mart_free_shipping_by_condition
        order by condition
        """,
    )


def get_top_sellers_by_volume(db_path: Path | str) -> list[dict[str, Any]]:
    return _fetch_rows(
        db_path,
        """
        select
            seller_nickname,
            product_count,
            total_sales_volume_proxy,
            avg_price,
            avg_discount_pct
        from mart_top_sellers_by_volume
        order by total_sales_volume_proxy desc, product_count desc
        limit 10
        """,
    )


def get_discount_volume_correlation(db_path: Path | str) -> dict[str, Any]:
    return _fetch_one(
        db_path,
        """
        select
            observations,
            discount_sales_volume_proxy_correlation,
            avg_discount_pct,
            avg_sales_volume_proxy
        from mart_discount_volume_correlation
        """,
    )


def get_discount_volume_scatter(db_path: Path | str, *, limit: int = 500) -> list[dict[str, Any]]:
    return _fetch_rows(
        db_path,
        """
        select
            item_id,
            title,
            seller_nickname,
            price,
            discount_pct,
            sales_volume_proxy
        from staging_product_current
        where discount_pct is not null
          and sales_volume_proxy is not null
        order by sales_volume_proxy desc, discount_pct desc
        limit ?
        """,
        (limit,),
    )


def get_avg_price_evolution(db_path: Path | str) -> list[dict[str, Any]]:
    return _fetch_rows(
        db_path,
        """
        select
            collected_date,
            product_count,
            avg_price,
            min_price,
            max_price
        from mart_avg_price_evolution
        order by collected_date
        """,
    )


def get_condition_distribution(db_path: Path | str) -> list[dict[str, Any]]:
    return _fetch_rows(
        db_path,
        """
        select
            condition,
            product_count,
            avg_ticket,
            total_sales_volume_proxy
        from mart_condition_distribution
        order by product_count desc, condition
        """,
    )


def get_price_variation_products(db_path: Path | str) -> list[dict[str, Any]]:
    mart_columns = _table_columns(db_path, "mart_price_variation_products")
    expected_columns = {"first_price", "last_price", "absolute_price_variation"}
    if not expected_columns.issubset(mart_columns):
        return _fetch_rows(
            db_path,
            """
            with ranked as (
                select
                    *,
                    row_number() over (
                        partition by item_id
                        order by collected_at asc, payload_hash asc
                    ) as first_rn,
                    row_number() over (
                        partition by item_id
                        order by collected_at desc, payload_hash desc
                    ) as last_rn,
                    count(*) over (partition by item_id) as observations
                from fact_product_prices
            ),
            first_prices as (
                select
                    item_id,
                    collected_at as first_collected_at,
                    price as first_price
                from ranked
                where first_rn = 1
            ),
            last_prices as (
                select
                    item_id,
                    title,
                    brand,
                    observations,
                    collected_at as last_collected_at,
                    price as last_price
                from ranked
                where last_rn = 1
            )
            select
                l.item_id,
                l.title,
                l.brand,
                l.observations,
                f.first_collected_at,
                l.last_collected_at,
                f.first_price,
                l.last_price,
                l.last_price - f.first_price as price_variation,
                abs(l.last_price - f.first_price) as absolute_price_variation
            from last_prices l
            join first_prices f using (item_id)
            order by absolute_price_variation desc, observations desc
            limit 20
            """,
        )

    return _fetch_rows(
        db_path,
        """
        select
            item_id,
            title,
            brand,
            observations,
            first_collected_at,
            last_collected_at,
            first_price,
            last_price,
            price_variation,
            absolute_price_variation
        from mart_price_variation_products
        order by absolute_price_variation desc, observations desc
        limit 20
        """,
    )


def get_free_shipping_price_comparison(db_path: Path | str) -> list[dict[str, Any]]:
    return _fetch_rows(
        db_path,
        """
        select
            free_shipping_status,
            product_count,
            avg_price,
            min_price,
            max_price
        from mart_free_shipping_price_comparison
        order by free_shipping_status
        """,
    )


def get_price_bands(db_path: Path | str) -> list[dict[str, Any]]:
    return _fetch_rows(
        db_path,
        """
        select
            price_band_start,
            price_band_end,
            product_count,
            avg_price,
            concat('R$ ', price_band_start::varchar, ' a R$ ', price_band_end::varchar) as price_band_label
        from mart_price_bands
        order by price_band_start
        """,
    )


def get_seller_price_volume_balance(db_path: Path | str) -> list[dict[str, Any]]:
    return _fetch_rows(
        db_path,
        """
        select
            seller_nickname,
            product_count,
            avg_price,
            total_sales_volume_proxy,
            price_volume_balance_score
        from mart_seller_price_volume_balance
        order by price_volume_balance_score desc, total_sales_volume_proxy desc
        limit 10
        """,
    )


def load_dashboard_data(db_path: Path | str) -> dict[str, Any]:
    return {
        "last_collection": get_last_collection_info(db_path),
        "price_kpis": get_price_kpis(db_path),
        "free_shipping_by_condition": get_free_shipping_by_condition(db_path),
        "top_sellers_by_volume": get_top_sellers_by_volume(db_path),
        "discount_volume_correlation": get_discount_volume_correlation(db_path),
        "discount_volume_scatter": get_discount_volume_scatter(db_path),
        "avg_price_evolution": get_avg_price_evolution(db_path),
        "condition_distribution": get_condition_distribution(db_path),
        "price_variation_products": get_price_variation_products(db_path),
        "free_shipping_price_comparison": get_free_shipping_price_comparison(db_path),
        "price_bands": get_price_bands(db_path),
        "seller_price_volume_balance": get_seller_price_volume_balance(db_path),
    }
