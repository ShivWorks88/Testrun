# Self-Reconciling Data Pipeline POC: shortcuts. Run `make help`.
SHELL := /bin/bash
DATE ?= $(shell python3 -c "import datetime as d; print(d.date.today()-d.timedelta(days=1))")
DAYS ?= 1
SCALE ?= 1.0
-include .env
export

.PHONY: help
help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n",$$1,$$2}'

# ------------------------------------------------------------------ Phase 0: cloud foundations
.PHONY: tf-init tf-plan tf-apply tf-destroy env
tf-init:  ## Download the Terraform providers
	terraform -chdir=infra init
tf-plan:  ## Show what Terraform would change
	terraform -chdir=infra plan
tf-apply:  ## Create/update the GCP environment (about 15 minutes the first time)
	terraform -chdir=infra apply
tf-destroy:  ## Delete the GCP environment (archive objects under retention stay until they expire)
	terraform -chdir=infra destroy
env:  ## Write .env from Terraform outputs
	./scripts/write_env.sh

# ------------------------------------------------------------------ local runtime
.PHONY: up down logs init-source generate status
up:  ## Start Postgres (source replicas) and Airflow on http://localhost:8080
	docker compose up -d --build
down:  ## Stop the local containers (data kept)
	docker compose down
logs:  ## Follow Airflow logs
	docker compose logs -f airflow
generate:  ## Generate synthetic source data: make generate DATE=2026-10-13 DAYS=3
	docker compose exec airflow python -m datagen --scale $(SCALE) generate --date $(DATE) --days $(DAYS)
status:  ## Show generated dates and source row counts
	docker compose exec airflow python -m datagen status

# ------------------------------------------------------------------ Phase 1: ingest
.PHONY: trigger ingest replay verify
trigger:  ## Trigger all three ingest DAGs for DATE
	for d in ingest_ehr ingest_lis ingest_billing; do \
	  docker compose exec airflow airflow dags unpause $$d >/dev/null; \
	  docker compose exec airflow airflow dags trigger $$d --conf '{"business_date":"$(DATE)"}'; done
ingest:  ## Ingest DATE from the command line (no Airflow): make ingest DATE=2026-10-13
	docker compose exec airflow python -m reconcile_ingest extract --date $(DATE)
replay:  ## Replay from the archive: make replay DATASET=encounters DATE=2026-10-13
	docker compose exec airflow python -m reconcile_ingest replay --dataset $(DATASET) --date $(DATE)
verify:  ## Run the Phase 1 exit-gate checks for DATE
	docker compose exec airflow python -m reconcile_ingest verify --date $(DATE)

# ------------------------------------------------------------------ console
.PHONY: deploy-console console api-local web-dev
deploy-console:  ## Build with Cloud Build and deploy the console + API to Cloud Run
	./scripts/deploy_console.sh
console:  ## Open the deployed console at http://localhost:8081 (authenticated proxy)
	gcloud run services proxy reconcile-console --project $(GCP_PROJECT) --region $(GCP_REGION) --port 8081
api-local:  ## Run the API on your laptop as sa-api (http://localhost:8000/docs)
	cd api && ENV=local IMPERSONATE_SERVICE_ACCOUNT=$(SA_API) \
	  DATABASE_URL=postgresql+asyncpg://reconcile:reconcile@localhost:5433/ops_state \
	  uvicorn app.main:app --reload --port 8000
web-dev:  ## Run the console dev server on http://localhost:5173 (proxies /api to :8000)
	cd web && npm install && npm run dev

# ------------------------------------------------------------------ checks (same as CI)
.PHONY: test lint check
test:  ## Run all unit tests (needs the docker-compose Postgres for ingest tests)
	cd ingest && python -m pytest -q tests
	cd api && python -m pytest -q tests
	python -m pytest -q tests
	cd web && npx vitest run
lint:  ## Lint Python
	ruff check .
check:  ## Policy schema, dbt sources in sync, dbt parse
	python scripts/check_policy.py
	python scripts/gen_dbt_sources.py --check
	cd dbt/reconcile && dbt parse --profiles-dir .

.PHONY: dev-setup
dev-setup:  ## Install Python packages and console dependencies for local tests
	python3 -m pip install -e ./data-gen -e "./ingest[dev]" -e "./api[dev]" ruff dbt-core dbt-bigquery
	cd web && npm ci
