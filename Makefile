.PHONY: help up down install test lint ingest transform dagster clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

up:  ## Start Mongo + MinIO and create the buckets
	docker compose up -d mongo minio minio-init

down:  ## Stop all containers
	docker compose down

install:  ## Install the package with dev extras
	pip install -e ".[dev]"

test:  ## Run the test suite
	pytest

check-site:  ## Verify the live site still matches the adapter (no infra needed)
	python scripts/check_site_contract.py

lint:  ## Lint and type-check
	ruff check src tests && mypy src

ingest:  ## Crawl a date window, e.g. make ingest START=2024-01-01 END=2024-03-01
	python -m wrc_pipeline.scraping.runner --start-date $(START) --end-date $(END)

transform:  ## Transform a date window
	python -m wrc_pipeline.transform.job --start-date $(START) --end-date $(END)

dagster:  ## Launch the Dagster UI on localhost:3000
	dagster dev -m wrc_pipeline.orchestration.definitions

clean:  ## Remove containers and volumes (destroys local data)
	docker compose down -v
