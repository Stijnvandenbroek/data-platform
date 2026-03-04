"""Elementary jobs."""

from dagster import job

from data_platform.ops.elementary import (
    elementary_generate_report,
    elementary_run_models,
)


@job(
    description="Ensure Elementary models exist, then regenerate the observability report."
)
def elementary_refresh_job():
    elementary_generate_report(after=elementary_run_models())
