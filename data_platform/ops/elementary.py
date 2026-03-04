"""Elementary ops."""

import os
import subprocess
from pathlib import Path

from dagster import In, Nothing, OpExecutionContext, op
from dagster_dbt import DbtCliResource
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
    engine = create_engine(url)
    with engine.connect() as conn:
        return bool(
            conn.execute(
                text(
                    "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'elementary'"
                )
            ).scalar()
        )


@op
def elementary_run_models(context: OpExecutionContext, dbt: DbtCliResource) -> None:
    """Run Elementary dbt models only if the elementary schema does not exist yet."""
    if _elementary_schema_exists():
        context.log.info("Elementary schema already exists, skipping model run.")
        return

    context.log.info("Elementary schema not found, running dbt models.")
    list(
        dbt.cli(
            ["run", "--select", "elementary"],
            context=context,
        ).stream()
    )


@op(ins={"after": In(Nothing)})
def elementary_generate_report(context: OpExecutionContext) -> None:
    """Run edr report to regenerate the Elementary HTML report."""
    cmd = [
        "edr",
        "report",
        "--profiles-dir",
        str(_DBT_DIR),
        "--project-dir",
        str(_DBT_DIR),
        "--disable-open-browser",
    ]
    context.log.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout:
        context.log.info(result.stdout)
    if result.stderr:
        context.log.warning(result.stderr)
    if result.returncode != 0:
        raise Exception(f"edr report failed with exit code {result.returncode}")
    context.log.info("Elementary report generated successfully.")
