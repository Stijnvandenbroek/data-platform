#!/bin/sh
set -e

echo "Generating dbt manifest..."
dbt parse --profiles-dir /app/dbt --project-dir /app/dbt

exec "$@"
