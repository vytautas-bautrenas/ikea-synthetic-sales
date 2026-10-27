SHELL := /bin/bash

.PHONY: env terra-apply terra-destroy ingest-onspd ingest-prod-cat \
	ingest-stores source-names ingest-names source-streets ingest-streets \
	source-onspd source-postcode-areas source-prod-cat dbt-run dbt-compile dbt-test dbt-audit \
	dbt-parse source-stores dbt-docs invoice heatmap \
	airflow-up airflow-down airflow-trigger airflow-logs


env:
	cp -n .env_example .env

terra-apply:
	source .env && cd terraform && terraform init && terraform apply

terra-destroy:
	source .env && cd terraform && terraform destroy


source-names:
	cd python && uv run source_names.py

source-onspd:
	cd python && uv run source_onspd.py

source-postcode-areas:
	cd python && uv run source_postcode_areas.py

source-prod-cat:
	cd python && uv run source_prod_cat.py

source-streets:
	cd python && uv run source_streets.py

source-stores:
	cd python && uv run source_stores.py


ingest-names:
	source .env && cd python && uv run ingest_names.py

ingest-onspd:
	source .env && cd python && uv run ingest_onspd.py

ingest-prod-cat:
	source .env && cd python && uv run ingest_prod_cat.py

ingest-stores:
	source .env && cd python && uv run ingest_stores.py

ingest-streets:
	source .env && cd python && uv run ingest_streets.py


dbt-run:
	source .env && cd dbt && dbt run $(if $(SELECT),--select $(SELECT))

dbt-parse:
	source .env && cd dbt && dbt parse

dbt-compile:
	source .env && cd dbt && dbt compile $(if $(SELECT),--select $(SELECT))

dbt-docs:
	source .env && cd dbt && dbt docs generate && dbt docs serve

dbt-test:
	source .env && cd dbt && dbt test $(if $(SELECT),--select $(SELECT))

dbt-audit:
	source .env && cd dbt && dbt run --select models/audit


invoice:
	source .env && cd web_app_invoicing && \
	uv run uvicorn main:app --reload --port 8000

heatmap:
	source .env && cd web_app_heatmap && \
	uv run uvicorn main:app --reload --port 8001


airflow-up:
	source .env && cd airflow && docker compose up --build -d

airflow-down:
	source .env && cd airflow && docker compose down -v

airflow-trigger:
	source .env && cd airflow && \
	docker compose exec airflow-scheduler \
	airflow dags trigger ikea_sales_pipeline

airflow-logs:
	source .env && cd airflow && \
	docker compose logs -f airflow-scheduler