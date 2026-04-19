from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


BRONZE_TABLE = "bronze_product_listing_events"
RAW_COMPAT_TABLE = "raw_product_listing_events"


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


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


def create_bronze_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        f"""
        create table if not exists {BRONZE_TABLE} (
            payload_hash varchar primary key,
            source varchar not null,
            item_id varchar not null,
            collection_run_id varchar,
            collected_at varchar,
            ingested_at varchar not null,
            raw_json varchar not null
        )
        """
    )

    # Migra bancos antigos que ainda tinham a camada raw como tabela física.
    if _table_exists(conn, RAW_COMPAT_TABLE):
        try:
            conn.execute(
                f"""
                insert into {BRONZE_TABLE}
                select
                    payload_hash, source, item_id, collection_run_id,
                    collected_at, ingested_at, raw_json
                from {RAW_COMPAT_TABLE}
                where payload_hash not in (select payload_hash from {BRONZE_TABLE})
                """
            )
        except duckdb.Error:
            # Se o nome antigo for uma view ou estiver com schema incompatível,
            # a tabela bronze já continua sendo a fonte canônica.
            pass


def create_raw_table(conn: duckdb.DuckDBPyConnection) -> None:
    create_bronze_table(conn)


def insert_event_to_bronze(
    conn: duckdb.DuckDBPyConnection,
    *,
    payload: dict[str, Any],
    ingested_at: str,
) -> bool:
    payload_hash = payload["payload_hash"]
    try:
        conn.execute(
            f"""
            insert into {BRONZE_TABLE} (
                payload_hash, source, item_id, collection_run_id,
                collected_at, ingested_at, raw_json
            )
            values (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload_hash,
                payload.get("source"),
                payload.get("item_id"),
                payload.get("collection_run_id"),
                payload.get("collected_at"),
                ingested_at,
                json.dumps(payload, ensure_ascii=False, default=str),
            ),
        )
        return True
    except duckdb.ConstraintException:
        return False


def consume_queue_to_raw(*, queue_path: Path, db_path: Path) -> dict[str, Any]:
    conn = connect(db_path)
    create_bronze_table(conn)

    inserted = 0
    skipped_existing = 0
    invalid = 0
    ingested_at = datetime.now(timezone.utc).isoformat()

    with queue_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                envelope = json.loads(line)
                payload = envelope["payload"]
                payload["payload_hash"]
            except Exception:
                invalid += 1
                continue

            if insert_event_to_bronze(conn, payload=payload, ingested_at=ingested_at):
                inserted += 1
            else:
                skipped_existing += 1

    conn.close()
    return {
        "inserted": inserted,
        "skipped_existing": skipped_existing,
        "invalid": invalid,
        "db_path": str(db_path),
        "table": BRONZE_TABLE,
    }


def transform_bronze_to_staging(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        f"""
        drop table if exists staging_product_listings;
        create table staging_product_listings as
        with typed as (
            select
                payload_hash,
                json_extract_string(raw_json, '$.source') as source,
                json_extract_string(raw_json, '$.site_id') as site_id,
                json_extract_string(raw_json, '$.category_id') as category_id,
                json_extract_string(raw_json, '$.search_query') as search_query,
                json_extract_string(raw_json, '$.collection_run_id') as collection_run_id,
                json_extract_string(raw_json, '$.collected_at') as collected_at,
                coalesce(
                    nullif(json_extract_string(raw_json, '$.collected_date'), ''),
                    substr(json_extract_string(raw_json, '$.collected_at'), 1, 10)
                ) as collected_date,
                json_extract_string(raw_json, '$.item_id') as item_id,
                trim(json_extract_string(raw_json, '$.title')) as title,
                json_extract_string(raw_json, '$.permalink') as permalink,
                coalesce(nullif(json_extract_string(raw_json, '$.seller_nickname'), ''), 'Não identificado') as seller_nickname,
                try_cast(nullif(json_extract_string(raw_json, '$.price'), '') as double) as price,
                try_cast(nullif(json_extract_string(raw_json, '$.original_price'), '') as double) as original_price,
                try_cast(nullif(json_extract_string(raw_json, '$.pix_price'), '') as double) as pix_price,
                try_cast(nullif(json_extract_string(raw_json, '$.discount_amount'), '') as double) as discount_amount,
                try_cast(nullif(json_extract_string(raw_json, '$.discount_pct'), '') as double) as discount_pct,
                try_cast(nullif(json_extract_string(raw_json, '$.installment_count'), '') as integer) as installment_count,
                try_cast(nullif(json_extract_string(raw_json, '$.installment_value'), '') as double) as installment_value,
                try_cast(nullif(json_extract_string(raw_json, '$.rating'), '') as double) as rating,
                try_cast(nullif(json_extract_string(raw_json, '$.review_count'), '') as integer) as review_count,
                coalesce(nullif(lower(json_extract_string(raw_json, '$.condition')), ''), 'new') as condition,
                try_cast(nullif(json_extract_string(raw_json, '$.free_shipping'), '') as boolean) as free_shipping,
                coalesce(nullif(json_extract_string(raw_json, '$.brand'), ''), 'Não identificado') as brand,
                nullif(json_extract_string(raw_json, '$.storage_capacity'), '') as storage_capacity,
                nullif(json_extract_string(raw_json, '$.ram_memory'), '') as ram_memory,
                try_cast(nullif(json_extract_string(raw_json, '$.sold_quantity'), '') as integer) as sold_quantity,
                raw_json
            from {BRONZE_TABLE}
        )
        select
            *,
            coalesce(sold_quantity, review_count, 0) as sales_volume_proxy
        from typed
        where price is not null
          and price > 0
          and item_id is not null
          and title is not null
          and (
              regexp_matches(lower(title), '(smartphone|celular|iphone|galaxy|motorola|samsung|xiaomi|redmi|moto g)')
              or brand in ('Samsung', 'Motorola', 'Apple', 'Xiaomi')
          )
          and not regexp_matches(lower(title), '(capa|capinha|pel[ií]cula|fone|buds|watch|carregador|cabo|case protetora)');
        """
    )

    conn.execute(
        """
        drop table if exists staging_product_current;
        create table staging_product_current as
        select *
        from (
            select
                *,
                row_number() over (
                    partition by item_id
                    order by collected_at desc, sales_volume_proxy desc, payload_hash
                ) as rn
            from staging_product_listings
        )
        where rn = 1;
        """
    )


def transform_staging_to_marts(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        drop table if exists fact_product_prices;

        drop table if exists dim_products;
        create table dim_products (
            item_id varchar primary key,
            title varchar,
            permalink varchar,
            brand varchar,
            storage_capacity varchar,
            ram_memory varchar,
            first_seen_date varchar,
            last_seen_date varchar,
            observations bigint
        );

        insert into dim_products
        select
            item_id,
            any_value(title) as title,
            any_value(permalink) as permalink,
            any_value(brand) as brand,
            any_value(storage_capacity) as storage_capacity,
            any_value(ram_memory) as ram_memory,
            min(collected_date) as first_seen_date,
            max(collected_date) as last_seen_date,
            count(*) as observations
        from staging_product_listings
        group by item_id;
        """
    )

    conn.execute(
        """
        drop table if exists dim_brands;
        create table dim_brands (
            brand varchar primary key,
            product_count bigint,
            avg_price double,
            min_price double,
            max_price double,
            avg_discount_pct double,
            total_sales_volume_proxy hugeint
        );

        insert into dim_brands
        select
            brand,
            count(distinct item_id) as product_count,
            avg(price) as avg_price,
            min(price) as min_price,
            max(price) as max_price,
            avg(discount_pct) as avg_discount_pct,
            sum(sales_volume_proxy) as total_sales_volume_proxy
        from staging_product_listings
        group by brand;
        """
    )

    conn.execute(
        """
        drop table if exists dim_sellers;
        create table dim_sellers (
            seller_nickname varchar primary key,
            product_count bigint,
            avg_price double,
            min_price double,
            max_price double,
            avg_discount_pct double,
            total_sales_volume_proxy hugeint
        );

        insert into dim_sellers
        select
            seller_nickname,
            count(distinct item_id) as product_count,
            avg(price) as avg_price,
            min(price) as min_price,
            max(price) as max_price,
            avg(discount_pct) as avg_discount_pct,
            sum(sales_volume_proxy) as total_sales_volume_proxy
        from staging_product_listings
        group by seller_nickname;
        """
    )

    conn.execute(
        """
        drop table if exists dim_conditions;
        create table dim_conditions (
            condition varchar primary key,
            product_count bigint,
            avg_price double,
            total_sales_volume_proxy hugeint
        );

        insert into dim_conditions
        select
            condition,
            count(distinct item_id) as product_count,
            avg(price) as avg_price,
            sum(sales_volume_proxy) as total_sales_volume_proxy
        from staging_product_listings
        group by condition;
        """
    )

    conn.execute(
        """
        drop table if exists fact_product_prices;
        create table fact_product_prices (
            payload_hash varchar primary key,
            item_id varchar not null,
            collected_date varchar,
            collected_at varchar,
            source varchar,
            search_query varchar,
            brand varchar,
            seller_nickname varchar,
            title varchar,
            permalink varchar,
            price double,
            original_price double,
            pix_price double,
            discount_amount double,
            discount_pct double,
            installment_count integer,
            installment_value double,
            rating double,
            review_count integer,
            sold_quantity integer,
            sales_volume_proxy integer,
            condition varchar,
            free_shipping boolean,
            storage_capacity varchar,
            ram_memory varchar,
            foreign key (item_id) references dim_products(item_id),
            foreign key (brand) references dim_brands(brand),
            foreign key (seller_nickname) references dim_sellers(seller_nickname),
            foreign key (condition) references dim_conditions(condition)
        );

        insert into fact_product_prices
        select
            payload_hash,
            item_id,
            collected_date,
            collected_at,
            source,
            search_query,
            brand,
            seller_nickname,
            title,
            permalink,
            price,
            original_price,
            pix_price,
            discount_amount,
            discount_pct,
            installment_count,
            installment_value,
            rating,
            review_count,
            sold_quantity,
            sales_volume_proxy,
            condition,
            free_shipping,
            storage_capacity,
            ram_memory
        from staging_product_listings;
        """
    )

    conn.execute(
        """
        drop table if exists mart_price_summary;
        create table mart_price_summary as
        select
            collected_date,
            count(distinct item_id) as total_products,
            avg(price) as avg_price,
            min(price) as min_price,
            max(price) as max_price,
            avg(discount_pct) as avg_discount_pct,
            avg(rating) as avg_rating,
            sum(sales_volume_proxy) as total_sales_volume_proxy
        from staging_product_current
        group by collected_date;
        """
    )

    conn.execute(
        """
        drop table if exists mart_free_shipping_by_condition;
        create table mart_free_shipping_by_condition as
        select
            condition,
            count(*) as product_count,
            sum(case when free_shipping is true then 1 else 0 end) as free_shipping_count,
            sum(case when free_shipping is false then 1 else 0 end) as paid_shipping_count,
            sum(case when free_shipping is null then 1 else 0 end) as unknown_shipping_count,
            free_shipping_count::double / nullif(product_count - unknown_shipping_count, 0) as free_shipping_pct_known
        from staging_product_current
        group by condition;
        """
    )

    conn.execute(
        """
        drop table if exists mart_top_sellers_by_volume;
        create table mart_top_sellers_by_volume as
        select
            seller_nickname,
            count(distinct item_id) as product_count,
            sum(sales_volume_proxy) as total_sales_volume_proxy,
            avg(price) as avg_price,
            avg(discount_pct) as avg_discount_pct
        from staging_product_current
        group by seller_nickname
        order by total_sales_volume_proxy desc, product_count desc
        limit 10;
        """
    )

    conn.execute(
        """
        drop table if exists mart_discount_volume_correlation;
        create table mart_discount_volume_correlation as
        select
            count(*) as observations,
            corr(discount_pct, sales_volume_proxy) as discount_sales_volume_proxy_correlation,
            avg(discount_pct) as avg_discount_pct,
            avg(sales_volume_proxy) as avg_sales_volume_proxy
        from staging_product_current
        where discount_pct is not null
          and sales_volume_proxy is not null;
        """
    )

    conn.execute(
        """
        drop table if exists mart_avg_price_evolution;
        create table mart_avg_price_evolution as
        select
            collected_date,
            count(distinct item_id) as product_count,
            avg(price) as avg_price,
            min(price) as min_price,
            max(price) as max_price
        from fact_product_prices
        group by collected_date
        order by collected_date;
        """
    )

    conn.execute(
        """
        drop table if exists mart_condition_distribution;
        create table mart_condition_distribution as
        select
            condition,
            count(distinct item_id) as product_count,
            avg(price) as avg_ticket,
            sum(sales_volume_proxy) as total_sales_volume_proxy
        from staging_product_current
        group by condition;
        """
    )

    conn.execute(
        """
        drop table if exists mart_price_variation_products;
        create table mart_price_variation_products as
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
        limit 20;
        """
    )

    conn.execute(
        """
        drop table if exists mart_free_shipping_price_comparison;
        create table mart_free_shipping_price_comparison as
        select
            coalesce(cast(free_shipping as varchar), 'unknown') as free_shipping_status,
            count(*) as product_count,
            avg(price) as avg_price,
            min(price) as min_price,
            max(price) as max_price
        from staging_product_current
        group by coalesce(cast(free_shipping as varchar), 'unknown');
        """
    )

    conn.execute(
        """
        drop table if exists mart_price_bands;
        create table mart_price_bands as
        with priced as (
            select
                price,
                cast(floor(price / 500) as integer) as price_band
            from staging_product_current
        )
        select
            price_band * 500 as price_band_start,
            price_band * 500 + 499 as price_band_end,
            count(*) as product_count,
            avg(price) as avg_price
        from priced
        group by price_band
        order by price_band_start;
        """
    )

    conn.execute(
        """
        drop table if exists mart_seller_price_volume_balance;
        create table mart_seller_price_volume_balance as
        select
            seller_nickname,
            count(distinct item_id) as product_count,
            avg(price) as avg_price,
            sum(sales_volume_proxy) as total_sales_volume_proxy,
            sum(sales_volume_proxy) / nullif(avg(price), 0) as price_volume_balance_score
        from staging_product_current
        group by seller_nickname
        order by price_volume_balance_score desc, total_sales_volume_proxy desc
        limit 10;
        """
    )

    conn.execute(
        """
        drop table if exists mart_top_reviewed_products;
        create table mart_top_reviewed_products as
        select
            item_id,
            title,
            brand,
            price,
            discount_pct,
            rating,
            review_count,
            sales_volume_proxy,
            permalink
        from staging_product_current
        where review_count is not null
        order by review_count desc
        limit 20;
        """
    )


def table_counts(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    tables = [
        BRONZE_TABLE,
        RAW_COMPAT_TABLE,
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
    counts: dict[str, int] = {}
    for table in tables:
        if _table_exists(conn, table):
            counts[table] = conn.execute(f"select count(*) from {table}").fetchone()[0]
        elif table == RAW_COMPAT_TABLE and _table_exists(conn, BRONZE_TABLE):
            counts[table] = conn.execute(f"select count(*) from {BRONZE_TABLE}").fetchone()[0]
    return counts


def run_transformations(*, db_path: Path) -> dict[str, Any]:
    conn = connect(db_path)
    create_bronze_table(conn)
    transform_bronze_to_staging(conn)
    transform_staging_to_marts(conn)
    counts = table_counts(conn)
    conn.close()
    return {"db_path": str(db_path), "table_counts": counts}
