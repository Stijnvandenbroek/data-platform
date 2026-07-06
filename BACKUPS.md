This file documents the backup setup for the central PostgreSQL instance on the `data` VM
(10.0.0.108) used by the data-platform project.

## What is backed up

- PostgreSQL globals: roles, tablespaces, and configuration (`globals.sql.gz`).
- Per-database logical dumps: `dagster`, `mlflow`, and `postgres`.

## Where backups live

Host path:

    /home/stijn/git/data-platform/backups

Container mount point:

    /app/backups

This directory is bind-mounted into the `postgres` container at `/app/backups` so that the
container's `postgres` user can write dumps without host permission issues. On the host the
directory is owned by `stijn:stijn` with mode `750`. Inside each daily run directory files are mode
`600`.

## Layout

    backups/
      backup.log
      daily/
        YYYYMMDD_HHMMSS/
          globals.sql.gz
          dagster.sql.gz
          mlflow.sql.gz
          postgres.sql.gz
          *.log

Old daily directories are removed after 14 days.

## Scripts and schedules

- `/home/stijn/git/data-platform/backup_postgres.sh` — performs the dumps and rotation. This is the
  canonical copy.
- Hermes cron job `data-platform-postgres-backup` (id `18058e6515cb`) — runs daily at 02:00 from the
  coding host (raspberrypi). It SSHs to the data VM as `stijn` and invokes the script above,
  appending output to `/home/stijn/git/data-platform/backups/backup.log`.

The script connects through the local container socket using the `dagster` superuser, so no password
file is required and no secrets are logged.

## Manual operation

Run the backup immediately on the data VM:

    ssh stijn@10.0.0.108
    cd /home/stijn/git/data-platform
    BACKUP_ROOT=/home/stijn/git/data-platform/backups PGUSER=dagster ./backup_postgres.sh

Check the latest log:

    cat /home/stijn/git/data-platform/backups/backup.log

List cron jobs:

    cronjob action='list'

## Restore procedure (summary)

1. Stop writes to the target database.
2. Create a fresh database if restoring to a new host.
3. Apply globals first if roles/tablespaces are missing:

   zcat <run>/globals.sql.gz | psql -U dagster -d postgres

4. Restore each database:

   zcat <run>/dagster.sql.gz | psql -U dagster -d dagster

For full migration to a new host, start the data-platform PostgreSQL container, create the databases
(or let `CREATE DATABASE` statements from the globals dump handle them), then run the per-database
restores.

## Verification

The script performs a `gzip -t` integrity check on every dump and checks that per-database dumps
contain the PostgreSQL dump header. Verification output is written to `verify.log` in the run
directory and echoed in the log.
