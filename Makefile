.PHONY: install test collect validate validate-warehouse pipeline report run-pipeline docker-up docker-build kafka-up

install:
	python -m pip install -r requirements.txt

test:
	python -m pytest

collect:
	python -m src.magalu.cli \
		--queries "$${QUERIES:-smartphone,iphone,samsung galaxy,motorola,xiaomi,celular 5g,galaxy,redmi}" \
		--pages "$${PAGES:-2}" \
		--limit "$${LIMIT:-40}" \
		--output "$${BRONZE_PATH:-data/bronze/magalu_smartphones.jsonl}" \
		--audit-output "$${AUDIT_PATH:-data/audit/collection_runs.jsonl}" \
		--log-level "$${LOG_LEVEL:-INFO}"

validate:
	python -m src.quality.bronze_validation \
		--bronze "$${BRONZE_PATH:-data/bronze/magalu_smartphones.jsonl}" \
		--min-rows "$${MIN_ROWS:-100}"

validate-warehouse:
	python -m src.quality.warehouse_validation \
		--db "$${DB_PATH:-data/warehouse/smartphone_price_intelligence.duckdb}"

pipeline:
	python -m src.pipeline.cli \
		--bronze "$${BRONZE_PATH:-data/bronze/magalu_smartphones.jsonl}" \
		--queue "$${QUEUE_PATH:-data/queue/product_listing_events.jsonl}" \
		--db "$${DB_PATH:-data/warehouse/smartphone_price_intelligence.duckdb}"

report:
	python -m src.reports.pipeline_report \
		--bronze "$${BRONZE_PATH:-data/bronze/magalu_smartphones.jsonl}" \
		--db "$${DB_PATH:-data/warehouse/smartphone_price_intelligence.duckdb}" \
		--audit "$${AUDIT_PATH:-data/audit/collection_runs.jsonl}" \
		--output "$${REPORT_PATH:-reports/pipeline_execution_report.md}" \
		--min-rows "$${MIN_ROWS:-100}"

run-pipeline:
	python -m src.orchestration.cli

kafka-up:
	docker compose up -d kafka

docker-build:
	docker compose build

docker-up:
	docker compose up --build
