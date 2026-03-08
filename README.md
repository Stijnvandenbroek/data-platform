# data-platform

A personal data platform for ingesting, transforming and analysing external data sources. Built with
[Dagster](https://dagster.io/), [dbt](https://www.getdbt.com/) and
[PostgreSQL](https://www.postgresql.org/), managed with [uv](https://github.com/astral-sh/uv) and
deployed via Docker Compose.

## Stack

| Layer          | Tool                                              |
| -------------- | ------------------------------------------------- |
| Orchestration  | Dagster (webserver + daemon)                      |
| Transformation | dbt-core + dbt-postgres                           |
| ML             | LightGBM, MLflow, scikit-learn                    |
| Storage        | PostgreSQL 16                                     |
| Notifications  | Discord webhooks                                  |
| Observability  | Elementary (report served via nginx)              |
| CI             | GitHub Actions (Ruff, SQLFluff, Prettier, pytest) |
| Package / venv | uv                                                |
| Infrastructure | Docker Compose                                    |

## Data pipeline

### Ingestion

Python-based Dagster assets fetch data from external APIs and load it into a raw PostgreSQL schema
using upsert semantics. Assets are prefixed with `raw_` to clearly separate unmodelled data from
transformed outputs. They are chained via explicit dependencies and run on a recurring cron
schedule.

### Transformation

dbt models follow the **staging → intermediate → marts** layering pattern:

| Layer            | Materialisation | Purpose                                                          |
| ---------------- | --------------- | ---------------------------------------------------------------- |
| **Staging**      | view            | 1:1 cleaning of each raw table with enforced contracts           |
| **Intermediate** | view            | Business-logic joins and enrichment across staging models        |
| **Marts**        | incremental     | Analysis-ready tables with derived metrics, loaded incrementally |

Mart models use dbt's **incremental materialisation** — on each run only rows with a newer ingestion
timestamp are merged into the target table. On a full-refresh the entire table is rebuilt.

All staging and intermediate models have **dbt contracts enforced**, locking column names and data
types to prevent silent schema drift.

### Data quality

- **dbt tests**: uniqueness, not-null, accepted values, referential integrity and expression-based
  tests run as part of every `dbt build`.
- **Source freshness**: a scheduled job verifies raw tables haven't gone stale.
- **Elementary**: collects test results and generates an HTML observability report served via nginx.

### Machine learning

An **ELO rating system** lets you rank listings via pairwise comparisons. An ML pipeline then learns
to predict ELO scores for unseen listings:

| Asset                  | Description                                                                         |
| ---------------------- | ----------------------------------------------------------------------------------- |
| `elo_prediction_model` | Trains a LightGBM regressor on listing features → ELO rating. Logs to MLflow.       |
| `elo_inference`        | Loads the best model from MLflow, scores all unscored listings, writes to Postgres. |
| `listing_alert`        | Sends a Discord notification for listings with a predicted ELO above a threshold.   |

All three are tagged `"manual"` — they run only when triggered explicitly.

### Notifications

The `listing_alert` asset posts rich embeds to a Discord channel via webhook when newly scored
listings exceed a configurable ELO threshold. Notifications are deduplicated using the
`elo.notified` table.

## Scheduling & automation

Ingestion assets run on cron schedules managed by the Dagster daemon. Downstream dbt models use
**eager auto-materialisation**: whenever an upstream raw asset completes, Dagster automatically
triggers the dbt build for all dependent staging, intermediate and mart models.

Assets tagged `"manual"` are excluded from auto-materialisation and only run when triggered
explicitly. A guard condition (`~any_deps_in_progress`) prevents duplicate runs while upstream
assets are still materialising.

## Project layout

```
data_platform/              # Dagster Python package
  assets/
    dbt.py                  # @dbt_assets definition
    elo/                    # ELO schema/table management assets
    ingestion/              # Raw ingestion assets + SQL templates
    ml/                     # ML assets (training, inference, alerts)
  helpers/                  # Shared utilities (SQL rendering, formatting, automation)
  jobs/                     # Job definitions
  schedules/                # Schedule definitions
  resources/                # Dagster resources (Postgres, MLflow, Discord, Funda)
  definitions.py            # Main Definitions entry point
dbt/                        # dbt project
  models/
    staging/                # 1:1 views on raw tables + source definitions
    intermediate/           # Enrichment joins
    marts/                  # Incremental analysis-ready tables
  macros/                   # Custom schema generation, Elementary compat
  profiles.yml              # Reads credentials from env vars
dagster_home/               # dagster.yaml + workspace.yaml
tests/                      # pytest test suite
nginx/                      # Elementary report nginx config
docker-compose.yaml         # All services
Dockerfile                  # Multi-stage: usercode + dagster-infra
Makefile                    # Developer shortcuts
```

## Getting started

```bash
# 1. Install uv (if not already)
curl -Lsf https://astral.sh/uv/install.sh | sh

# 2. Clone and enter the project
cd ~/git/data-platform

# 3. Create your credentials file
cp .env.example .env        # edit .env with your passwords

# 4. Install dependencies into a local venv
uv sync

# 5. Generate the dbt manifest (needed before first run)
uv run dbt deps  --project-dir dbt --profiles-dir dbt
uv run dbt parse --project-dir dbt --profiles-dir dbt

# 6. Start all services
docker compose up -d --build

# 7. Open the Dagster UI
#    http://localhost:3000
```

## Local development

```bash
uv sync
source .venv/bin/activate

# Run the Dagster UI locally
DAGSTER_HOME=$PWD/dagster_home dagster dev

# Useful Make targets
make validate          # Check Dagster definitions load
make lint              # Ruff + SQLFluff + Prettier
make lint-fix          # Auto-fix all linters
make test              # pytest
make reload-code       # Rebuild + restart user-code container
```

## Services

| Service           | URL                   |
| ----------------- | --------------------- |
| Dagster UI        | http://localhost:3000 |
| MLflow UI         | http://localhost:5000 |
| pgAdmin           | http://localhost:5050 |
| Elementary report | http://localhost:8080 |
