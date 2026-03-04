"""Elementary report generation op."""

import subprocess
from pathlib import Path

from dagster import OpExecutionContext, op

_DBT_DIR = Path(__file__).parents[2] / "dbt"


@op
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
