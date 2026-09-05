#!/usr/bin/env bash
# Central PostgreSQL backup script for data-platform postgres container.
# Runs via docker exec so the correct pg_dump/psql versions are always used
# and no PostgreSQL client install is needed on the host.
#
# Layout:
#   BACKUP_ROOT/
#     daily/
#       YYYYMMDD_HHMMSS/
#         globals.sql.gz
#         <database>.sql.gz
#
# Logs are written to stdout/stderr; the calling unit/cron should capture them
# via systemd journal or redirect to a log file. No secrets are echoed.

set -euo pipefail

# --- Configuration ---
BACKUP_ROOT="${BACKUP_ROOT:-/app/backups}"
# Default path is inside the postgres container, where /app/backups is a
# bind-mounted host directory. The host path is
# /home/stijn/git/data-platform/backups.  Override BACKUP_ROOT when running
# directly on the host (e.g. BACKUP_ROOT=/home/stijn/git/data-platform/backups).
PGUSER="${PGUSER:-dagster}"
# Connection uses the container's local socket; no password required.
export PGUSER

DUMP_OPTS=(
    --no-password
    --encoding=UTF8
    --verbose
)

PSQL_OPTS=(
    -At
    -U "${PGUSER}"
    --no-password
)

# Ensure the backup directory exists with safe permissions.
# When running inside the postgres container, BACKUP_ROOT is a named volume
# mounted at this path. Outside the container it may not exist; create it
# on the host so manual invocations from the shell also work.
mkdir -p "${BACKUP_ROOT}"
if [[ -d "${BACKUP_ROOT}" ]]; then
    chmod 750 "${BACKUP_ROOT}"
fi

# Ensure the daily directory exists with safe permissions.
mkdir -p "${BACKUP_ROOT}/daily"
chmod 750 "${BACKUP_ROOT}/daily"

RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${BACKUP_ROOT}/daily/${RUN_TS}"
mkdir -p "${RUN_DIR}"
chmod 700 "${RUN_DIR}"

log() {
    printf '%s %s\n' "$(date -Iseconds)" "$*"
}

cleanup() {
    local rc=$?
    if [[ ${rc} -ne 0 ]]; then
        log "ERROR: backup failed with exit code ${rc}; leaving ${RUN_DIR} for inspection"
    fi
    exit ${rc}
}
trap cleanup EXIT

log "Starting PostgreSQL logical backup to ${RUN_DIR}"

# --- Per-database dumps ---
mapfile -t DATABASES < <(docker exec postgres psql -At -U "${PGUSER}" --no-password -c "SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY datname;")

for db in "${DATABASES[@]}"; do
    log "Dumping database: ${db}"
    docker exec postgres pg_dump \
        --username "${PGUSER}" \
        --dbname "${db}" \
        --format=plain \
        "${DUMP_OPTS[@]}" \
        2> "${RUN_DIR}/${db}.log" \
        | gzip -c > "${RUN_DIR}/${db}.sql.gz"
    chmod 600 "${RUN_DIR}/${db}.sql.gz"
    log "Database ${db} dump complete ($(stat -c%s "${RUN_DIR}/${db}.sql.gz") bytes)"
done

# --- Globals (roles, tablespaces, configuration) ---
log "Dumping globals..."
docker exec postgres pg_dumpall \
    --globals-only \
    --username "${PGUSER}" \
    "${DUMP_OPTS[@]}" \
    2> "${RUN_DIR}/globals.log" \
    | gzip -c > "${RUN_DIR}/globals.sql.gz"
chmod 600 "${RUN_DIR}/globals.sql.gz"
log "Globals dump complete ($(stat -c%s "${RUN_DIR}/globals.sql.gz") bytes)"

# --- Lightweight integrity check: decompress and parse headers ---
log "Verifying dump integrity..."
for f in "${RUN_DIR}"/*.sql.gz; do
    [[ -e "${f}" ]] || continue
    if ! gzip -t "${f}" 2> "${RUN_DIR}/verify.log"; then
        log "ERROR: gzip integrity check failed for ${f}"
        exit 1
    fi
    if ! zgrep -q 'PostgreSQL database dump' "${f}" 2>/dev/null; then
        # pg_dump plain-format dumps contain a header line with the text above.
        # globals-only dump uses a slightly different header; be permissive.
        if [[ "$(basename "${f}")" != "globals.sql.gz" ]]; then
            log "WARNING: ${f} may be missing expected PostgreSQL dump header"
        fi
    fi
    log "OK: ${f}"
done

# --- Rotate old daily backups (keep 14 days) ---
log "Rotating daily backups older than 14 days..."
find "${BACKUP_ROOT}/daily" -mindepth 1 -maxdepth 1 -type d -mtime +14 -print0 | xargs -0 -r rm -rf

log "Backup completed successfully: ${RUN_DIR}"
