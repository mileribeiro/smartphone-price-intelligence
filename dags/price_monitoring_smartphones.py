from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from src.orchestration.tasks import (
    PipelineSettings,
    build_marts,
    build_staging,
    collect_and_publish,
    consume_stream_to_bronze,
    generate_report,
    validate_bronze,
    validate_final_warehouse,
)


@dag(
    dag_id="price_monitoring_smartphones",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["magalu", "smartphones", "prices"],
)
def price_monitoring_smartphones():
    @task
    def collect_and_publish_task():
        return collect_and_publish(PipelineSettings.from_env())

    @task
    def validate_bronze_task():
        return validate_bronze(PipelineSettings.from_env())

    @task
    def consume_kafka_to_bronze_task():
        return consume_stream_to_bronze(PipelineSettings.from_env())

    @task
    def build_staging_task():
        return build_staging(PipelineSettings.from_env())

    @task
    def build_marts_task():
        return build_marts(PipelineSettings.from_env())

    @task
    def validate_warehouse_task():
        return validate_final_warehouse(PipelineSettings.from_env())

    @task
    def generate_report_task():
        return generate_report(PipelineSettings.from_env())

    collected = collect_and_publish_task()
    bronze_validated = validate_bronze_task()
    consumed = consume_kafka_to_bronze_task()
    staged = build_staging_task()
    marts = build_marts_task()
    warehouse_validated = validate_warehouse_task()
    report = generate_report_task()

    collected >> bronze_validated >> consumed >> staged >> marts >> warehouse_validated >> report


price_monitoring_smartphones()
