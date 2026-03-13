#!/bin/sh
set -e

pip install --quiet "mlflow==3.10.1" psycopg2-binary

# Disable GenAI background job workers (saves ~700MB on small VMs)
export MLFLOW_SERVER_ENABLE_JOB_EXECUTION=false

# Ensure the mlflow database exists before starting the server.
# docker-entrypoint-initdb.d scripts only run on first init, so this
# handles existing postgres volumes where the database was never created.
python -c "
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

conn = psycopg2.connect(
    host='postgres',
    port=5432,
    user='${POSTGRES_USER}',
    password='${POSTGRES_PASSWORD}',
    dbname='${POSTGRES_DB}',
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()
cur.execute(\"SELECT 1 FROM pg_database WHERE datname = 'mlflow'\")
if not cur.fetchone():
    cur.execute('CREATE DATABASE mlflow')
    print('Created mlflow database')
else:
    print('mlflow database already exists')
conn.close()
"

exec mlflow server \
    --host=0.0.0.0 \
    --port=5000 \
    --backend-store-uri="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/mlflow" \
    --serve-artifacts \
    --artifacts-destination=/mlflow/artifacts \
    --default-artifact-root=mlflow-artifacts:/ \
    --allowed-hosts="*" \
    --cors-allowed-origins="*"
    -w 2
