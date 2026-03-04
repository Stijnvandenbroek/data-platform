# data-platform

A [Dagster](https://dagster.io/) + [dbt](https://www.getdbt.com/) data platform, managed with
[uv](https://github.com/astral-sh/uv) and deployed via Docker Compose.

## Stack

| Layer          | Tool                         |
| -------------- | ---------------------------- |
| Orchestration  | Dagster (webserver + daemon) |
| Transformation | dbt-core + dbt-postgres      |
| Storage        | PostgreSQL 16                |
| Package/venv   | uv                           |
| Secrets        | `.env` file                  |

## Project layout

```
data_platform/      # Dagster Python package (assets, definitions)
dbt/                # dbt project (models, seeds, tests)
  profiles.yml      # reads credentials from env vars
dagster_home/       # dagster.yaml + workspace.yaml
Dockerfile          # single image used by both dagster services
docker-compose.yaml # postgres + dagster-webserver + dagster-daemon
.env.example        # copy to .env and fill in credentials
pyproject.toml      # uv-managed dependencies
```

## Getting started

```bash
# 1. Install uv (if not already)
curl -Lsf https://astral.sh/uv/install.sh | sh

# 2. Clone and enter the project
cd ~/git/data-platform

# 3. Create your credentials file
cp .env.example .env
# Edit .env with your passwords

# 4. Install dependencies into a local venv
uv sync

# 5. Generate the dbt manifest (needed before first run)
uv run dbt parse --profiles-dir dbt --project-dir dbt

# 6. Start all services
docker compose up -d --build

# 7. Open the Dagster UI
#    http://localhost:3000
```

## Local development (without Docker)

```bash
uv sync
source .venv/bin/activate

# Run the Dagster UI locally
DAGSTER_HOME=$PWD/dagster_home dagster dev
```
