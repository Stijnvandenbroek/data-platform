#!/bin/sh
set -e

echo "Installing dbt packages..."
dbt deps --profiles-dir /app/dbt --project-dir /app/dbt

echo "Generating dbt manifest..."
dbt parse --profiles-dir /app/dbt --project-dir /app/dbt

exec "$@"
