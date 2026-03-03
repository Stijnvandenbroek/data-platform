.PHONY: help install sync build up down restart logs ps \
       build-code reload-code \
       dbt-parse dbt-run dbt-test dbt-build dbt-seed dbt-clean dbt-deps dbt-docs \
       dagster-dev lint test clean

# Default Docker Compose project containers
COMPOSE  := docker compose
DBT_DIR  := dbt
DBT      := uv run dbt
# Container(s) that carry user code
CODE_SERVICES := dagster-webserver dagster-daemon

## —— Help ——————————————————————————————————————————————————————
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

## —— Python / uv ——————————————————————————————————————————————
install: ## Install all dependencies (including dev) with uv
	uv sync

sync: install ## Alias for install

## —— Docker Compose ———————————————————————————————————————————
build: ## Build all Docker images
	$(COMPOSE) build

up: ## Start all services in the background
	$(COMPOSE) up -d

down: ## Stop and remove all services
	$(COMPOSE) down

restart: ## Restart all services
	$(COMPOSE) restart

logs: ## Tail logs for all services (ctrl-c to quit)
	$(COMPOSE) logs -f

ps: ## Show running containers
	$(COMPOSE) ps

## —— User-code container shortcuts ————————————————————————————
build-code: ## Rebuild only the user-code containers
	$(COMPOSE) build $(CODE_SERVICES)

reload-code: ## Rebuild and restart the user-code containers (no downtime for postgres)
	$(COMPOSE) build $(CODE_SERVICES)
	$(COMPOSE) up -d --no-deps $(CODE_SERVICES)

## —— dbt (local, via uv) —————————————————————————————————————
dbt-parse: ## Parse the dbt project and generate manifest.json
	$(DBT) parse --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

dbt-run: ## Run all dbt models
	$(DBT) run --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

dbt-test: ## Run dbt tests
	$(DBT) test --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

dbt-build: ## Run dbt build (run + test + snapshot + seed)
	$(DBT) build --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

dbt-seed: ## Load dbt seed files
	$(DBT) seed --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

dbt-clean: ## Clean dbt target and packages directories
	$(DBT) clean --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

dbt-deps: ## Install dbt packages
	$(DBT) deps --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

dbt-docs: ## Generate and serve dbt docs
	$(DBT) docs generate --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)
	$(DBT) docs serve --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

## —— Dagster (local dev) —————————————————————————————————————
dagster-dev: ## Start Dagster webserver locally for development
	uv run dagster dev

## —— Quality ——————————————————————————————————————————————————
lint: ## Run ruff linter and formatter check
	uv run ruff check .
	uv run ruff format --check .

test: ## Run pytest
	uv run pytest

## —— Misc —————————————————————————————————————————————————————
clean: ## Remove Python caches and build artefacts
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache dist *.egg-info
