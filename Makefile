# Single entry point for the project. `make help` lists targets.
SHELL := /bin/bash
.DEFAULT_GOAL := help

ENV ?= local
ROWS ?=
export FINTXN_ENV := $(ENV)

# Java 17 for Spark: honour an existing JAVA_HOME (CI), else macOS java_home, else Homebrew.
JAVA_HOME ?= $(shell /usr/libexec/java_home -v 17 2>/dev/null || echo /opt/homebrew/opt/openjdk@17)
export JAVA_HOME
export PYSPARK_PYTHON := $(CURDIR)/.venv/bin/python
export PYTHONUNBUFFERED := 1
export TZ := UTC

UV := uv run
CLI := $(UV) fintxn
ROWS_FLAG := $(if $(ROWS),--rows $(ROWS),)

.PHONY: mlflow-ui help setup up down generate bronze silver gold train score recon export pipeline \
        dashboard test test-fast lint fmt typecheck clean clean-data keys tf-fmt tf-validate \
        airflow-up airflow-down audit

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n",$$1,$$2}'

setup: ## Install deps, git hooks, create .env with random secrets
	uv sync --all-extras --frozen
	$(UV) pre-commit install
	python3 scripts/bootstrap_env.py

keys: ## (Re)render .env secrets and SeaweedFS identities (keeps existing values)
	python3 scripts/bootstrap_env.py

up: ## Start SeaweedFS (S3 API) and create buckets
	python3 scripts/bootstrap_env.py
	docker compose up -d seaweedfs
	docker compose run --rm bucket-init

down: ## Stop local infrastructure (data volume kept; `docker compose down -v` wipes it)
	docker compose --profile airflow down

generate: ## Generate synthetic transactions, dims, ledger, settlements (ROWS=n)
	$(CLI) generate $(ROWS_FLAG)

bronze: ## Raw files -> bronze Delta (incremental, idempotent)
	$(CLI) bronze

silver: ## Bronze -> silver (clean, validate, quarantine, encrypt PII)
	$(CLI) silver

gold: ## Silver -> point-in-time features + gold marts
	$(CLI) gold

train: ## Train LightGBM, log + register in MLflow
	$(CLI) train $(if $(filter local,$(ENV)),--results-file docs/results/model_metrics.json,)

score: ## Batch-score transactions and build scored marts
	$(CLI) score

recon: ## Ledger vs settlement reconciliation
	$(CLI) recon $(if $(filter local,$(ENV)),--results-file docs/results/recon_metrics.json,)

export: ## Export compact gold marts for Streamlit / Power BI
	$(CLI) export

audit: ## Print the pipeline audit trail
	$(CLI) audit-log

pipeline: ## Full pipeline end to end
	$(CLI) pipeline $(ROWS_FLAG) $(if $(filter local,$(ENV)),--results-dir docs/results,)

mlflow-ui: ## Browse MLflow runs and the model registry on http://localhost:5000
	$(UV) mlflow ui --backend-store-uri sqlite:///mlflow.db --port 5000

dashboard: ## Run the Streamlit dashboard
	$(UV) streamlit run dashboards/app.py

test: ## Unit + integration tests with coverage (local filesystem, 10k rows)
	FINTXN_ENV=ci $(UV) pytest --cov=fintxn --cov-report=term --cov-report=xml

test-fast: ## Unit tests that do not need Spark
	FINTXN_ENV=ci $(UV) pytest -m "not spark and not integration"

lint: ## Ruff lint + format check + mypy
	$(UV) ruff check .
	$(UV) ruff format --check .
	$(UV) mypy

fmt: ## Auto-format and fix lint
	$(UV) ruff format .
	$(UV) ruff check --fix .

typecheck: ## mypy only
	$(UV) mypy

tf-fmt: ## terraform fmt check
	terraform -chdir=infra/terraform fmt -check -recursive

tf-validate: ## terraform init (no backend) + validate
	terraform -chdir=infra/terraform init -backend=false -input=false >/dev/null
	terraform -chdir=infra/terraform validate

airflow-up: ## Start Airflow (optional profile) on http://localhost:8080
	docker compose --profile airflow up -d --build

airflow-down: ## Stop Airflow
	docker compose --profile airflow stop airflow airflow-db

clean: ## Remove caches and local build output
	find . -type d \( -name __pycache__ -o -name .pytest_cache -o -name .mypy_cache -o -name .ruff_cache \) -prune -exec rm -r {} +
	rm -f .coverage coverage.xml

clean-data: ## Remove local generated data (filesystem backend + MLflow); S3 volume untouched
	rm -r data/ci data/local mlruns mlflow.db 2>/dev/null || true
