"""Elementary ops."""

import os
import subprocess
from pathlib import Path

from dagster import In, Nothing, OpExecutionContext, op
from sqlalchemy import create_engine, text

_DBT_DIR = Path(__file__).parents[2] / "dbt"


def _elementary_schema_exists() -> bool:
    url = "postgresql://{user}:{password}@{host}:{port}/{dbname}".format(
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
    )
    engine = create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 10},
    )

    from data_platform.resources import _retry_on_operational_error

    def _query():
        with engine.connect() as conn:
            return bool(
                conn.execute(
                    text(
                        "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'elementary'"
                    )
                ).scalar()
            )

    return _retry_on_operational_error(_query)


@op
def elementary_run_models(context: OpExecutionContext) -> None:
    """Run Elementary dbt models only if the elementary schema does not exist yet."""
    if _elementary_schema_exists():
        context.log.info("Elementary schema already exists, skipping model run.")
        return

    context.log.info("Elementary schema not found, running dbt models.")
    cmd = [
        "dbt",
        "run",
        "--select",
        "elementary",
        "--profiles-dir",
        str(_DBT_DIR),
        "--project-dir",
        str(_DBT_DIR),
    ]
    context.log.info(f"Running: {' '.join(cmd)}")
    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    assert process.stdout is not None
    for line in process.stdout:
        context.log.info(line.rstrip())
    returncode = process.wait()
    if returncode != 0:
        raise Exception(f"dbt run elementary failed with exit code {returncode}")


@op(ins={"after": In(Nothing)})
def elementary_generate_report(context: OpExecutionContext) -> None:
    """Run edr report to regenerate the Elementary HTML report."""
    report_path = (
        Path(__file__).parents[2] / "dbt" / "edr_target" / "elementary_report.html"
    )
    cmd = [
        "edr",
        "report",
        "--profiles-dir",
        str(_DBT_DIR),
        "--project-dir",
        str(_DBT_DIR),
        "--file-path",
        str(report_path),
        "--days-back",
        "3",
        "--executions-limit",
        "30",
    ]
    context.log.info(f"Running: {' '.join(cmd)}")
    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    assert process.stdout is not None
    for line in process.stdout:
        context.log.info(line.rstrip())
    returncode = process.wait()
    if returncode != 0:
        raise Exception(f"edr report failed with exit code {returncode}")
    context.log.info("Elementary report generated successfully.")
