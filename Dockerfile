# Usercode: full application (gRPC server)
FROM python:3.12-slim AS usercode

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install system dependencies (git is required by elementary/dbt deps)
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

# Install dependencies before copying full source (layer caching)
COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-dev 2>/dev/null || uv sync --no-dev

# Copy application source
COPY . .

ENV PATH="/app/.venv/bin:$PATH"
ENV DAGSTER_HOME=/app/dagster_home

RUN chmod +x /app/entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]

# Infrastructure: webserver / daemon (no user code)
FROM python:3.12-slim AS dagster-infra

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# No application code, only dagster packages for webserver/daemon
RUN uv pip install --system dagster dagster-webserver dagster-postgres

COPY dagster_home/ ./dagster_home/

ENV DAGSTER_HOME=/app/dagster_home
