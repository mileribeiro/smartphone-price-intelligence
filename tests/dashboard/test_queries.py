from __future__ import annotations

import duckdb
import pytest

from src.dashboard.queries import (
    DashboardDataError,
    get_avg_price_evolution,
    get_discount_volume_correlation,
    get_price_bands,
    get_price_kpis,
    get_price_variation_products,
    get_top_sellers_by_volume,
    load_dashboard_data,
)


@pytest.fixture()
def dashboard_db(tmp_path):
    db_path = tmp_path / "dashboard.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            create table bronze_product_listing_events (
                payload_hash varchar,
                source varchar,
                item_id varchar,
                collection_run_id varchar,
                collected_at varchar,
                ingested_at varchar,
                raw_json varchar
            )
            """
        )
        conn.execute(
            """
            insert into bronze_product_listing_events values
                ('hash1', 'magalu', 'item1', 'run1', '2026-04-17T10:00:00+00:00', '2026-04-17T10:01:00+00:00', '{}'),
                ('hash2', 'magalu', 'item2', 'run2', '2026-04-18T10:00:00+00:00', '2026-04-18T10:01:00+00:00', '{}')
            """
        )
        conn.execute(
            """
            create table staging_product_current (
                item_id varchar,
                title varchar,
                seller_nickname varchar,
                price double,
                discount_pct double,
                sales_volume_proxy integer
            )
            """
        )
        conn.execute(
            """
            insert into staging_product_current values
                ('item1', 'Smartphone Samsung 128GB', 'seller-one', 1000.00, 10.0, 120),
                ('item2', 'Smartphone Motorola 256GB', 'seller-two', 1500.00, 20.0, 80)
            """
        )
        conn.execute(
            """
            create table mart_free_shipping_by_condition (
                condition varchar,
                product_count integer,
                free_shipping_count integer,
                paid_shipping_count integer,
                unknown_shipping_count integer,
                free_shipping_pct_known double
            )
            """
        )
        conn.execute(
            """
            insert into mart_free_shipping_by_condition values
                ('new', 2, 1, 1, 0, 0.5)
            """
        )
        conn.execute(
            """
            create table mart_top_sellers_by_volume (
                seller_nickname varchar,
                product_count integer,
                total_sales_volume_proxy integer,
                avg_price double,
                avg_discount_pct double
            )
            """
        )
        conn.execute(
            """
            insert into mart_top_sellers_by_volume values
                ('seller-one', 1, 120, 1000.00, 10.0),
                ('seller-two', 1, 80, 1500.00, 20.0)
            """
        )
        conn.execute(
            """
            create table mart_discount_volume_correlation (
                observations integer,
                discount_sales_volume_proxy_correlation double,
                avg_discount_pct double,
                avg_sales_volume_proxy double
            )
            """
        )
        conn.execute("insert into mart_discount_volume_correlation values (2, -1.0, 15.0, 100.0)")
        conn.execute(
            """
            create table mart_avg_price_evolution (
                collected_date varchar,
                product_count integer,
                avg_price double,
                min_price double,
                max_price double
            )
            """
        )
        conn.execute(
            """
            insert into mart_avg_price_evolution values
                ('2026-04-17', 1, 1000.00, 1000.00, 1000.00),
                ('2026-04-18', 2, 1250.00, 1000.00, 1500.00)
            """
        )
        conn.execute(
            """
            create table mart_condition_distribution (
                condition varchar,
                product_count integer,
                avg_ticket double,
                total_sales_volume_proxy integer
            )
            """
        )
        conn.execute("insert into mart_condition_distribution values ('new', 2, 1250.00, 200)")
        conn.execute(
            """
            create table mart_price_variation_products (
                item_id varchar,
                title varchar,
                brand varchar,
                observations integer,
                first_collected_at varchar,
                last_collected_at varchar,
                first_price double,
                last_price double,
                price_variation double,
                absolute_price_variation double
            )
            """
        )
        conn.execute(
            """
            insert into mart_price_variation_products values
                ('item1', 'Smartphone Samsung 128GB', 'Samsung', 2, '2026-04-17T10:00:00+00:00', '2026-04-18T10:00:00+00:00', 1100.00, 1000.00, -100.00, 100.00)
            """
        )
        conn.execute(
            """
            create table mart_free_shipping_price_comparison (
                free_shipping_status varchar,
                product_count integer,
                avg_price double,
                min_price double,
                max_price double
            )
            """
        )
        conn.execute(
            """
            insert into mart_free_shipping_price_comparison values
                ('true', 1, 1000.00, 1000.00, 1000.00),
                ('false', 1, 1500.00, 1500.00, 1500.00)
            """
        )
        conn.execute(
            """
            create table mart_price_bands (
                price_band_start integer,
                price_band_end integer,
                product_count integer,
                avg_price double
            )
            """
        )
        conn.execute(
            """
            insert into mart_price_bands values
                (1000, 1499, 1, 1000.00),
                (1500, 1999, 1, 1500.00)
            """
        )
        conn.execute(
            """
            create table mart_seller_price_volume_balance (
                seller_nickname varchar,
                product_count integer,
                avg_price double,
                total_sales_volume_proxy integer,
                price_volume_balance_score double
            )
            """
        )
        conn.execute(
            """
            insert into mart_seller_price_volume_balance values
                ('seller-one', 1, 1000.00, 120, 0.12),
                ('seller-two', 1, 1500.00, 80, 0.0533333333)
            """
        )
    finally:
        conn.close()
    return db_path


def test_price_kpis(dashboard_db):
    result = get_price_kpis(dashboard_db)

    assert result["product_count"] == 2
    assert result["avg_price"] == 1250
    assert result["min_price"] == 1000
    assert result["max_price"] == 1500


def test_top_sellers_by_volume(dashboard_db):
    result = get_top_sellers_by_volume(dashboard_db)

    assert result[0]["seller_nickname"] == "seller-one"
    assert result[0]["total_sales_volume_proxy"] == 120


def test_discount_volume_correlation(dashboard_db):
    result = get_discount_volume_correlation(dashboard_db)

    assert result["observations"] == 2
    assert result["discount_sales_volume_proxy_correlation"] == -1.0


def test_avg_price_evolution(dashboard_db):
    result = get_avg_price_evolution(dashboard_db)

    assert [row["collected_date"] for row in result] == ["2026-04-17", "2026-04-18"]
    assert result[-1]["avg_price"] == 1250


def test_price_variation_uses_first_and_last_records(dashboard_db):
    result = get_price_variation_products(dashboard_db)

    assert result[0]["first_price"] == 1100
    assert result[0]["last_price"] == 1000
    assert result[0]["price_variation"] == -100
    assert result[0]["absolute_price_variation"] == 100


def test_price_variation_falls_back_to_fact_when_mart_is_stale(tmp_path):
    db_path = tmp_path / "stale_mart.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            create table mart_price_variation_products (
                item_id varchar,
                title varchar,
                brand varchar,
                observations integer,
                min_price double,
                max_price double,
                price_variation double
            )
            """
        )
        conn.execute(
            """
            create table fact_product_prices (
                payload_hash varchar,
                item_id varchar,
                collected_at varchar,
                title varchar,
                brand varchar,
                price double
            )
            """
        )
        conn.execute(
            """
            insert into fact_product_prices values
                ('hash1', 'item1', '2026-04-17T10:00:00+00:00', 'Smartphone Samsung 128GB', 'Samsung', 1300.00),
                ('hash2', 'item1', '2026-04-18T10:00:00+00:00', 'Smartphone Samsung 128GB', 'Samsung', 1000.00)
            """
        )
    finally:
        conn.close()

    result = get_price_variation_products(db_path)

    assert result[0]["first_price"] == 1300
    assert result[0]["last_price"] == 1000
    assert result[0]["price_variation"] == -300


def test_price_bands(dashboard_db):
    result = get_price_bands(dashboard_db)

    assert result[0]["price_band_label"] == "R$ 1000 a R$ 1499"
    assert result[1]["product_count"] == 1


def test_load_dashboard_data_contains_all_answers(dashboard_db):
    result = load_dashboard_data(dashboard_db)

    assert set(result) == {
        "last_collection",
        "price_kpis",
        "free_shipping_by_condition",
        "top_sellers_by_volume",
        "discount_volume_correlation",
        "discount_volume_scatter",
        "avg_price_evolution",
        "condition_distribution",
        "price_variation_products",
        "free_shipping_price_comparison",
        "price_bands",
        "seller_price_volume_balance",
    }


def test_missing_database_has_friendly_error(tmp_path):
    with pytest.raises(DashboardDataError):
        get_price_kpis(tmp_path / "missing.duckdb")
