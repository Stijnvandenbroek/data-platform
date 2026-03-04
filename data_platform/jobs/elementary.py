"""Elementary jobs."""

from dagster import job

from data_platform.ops.elementary import elementary_generate_report


@job(description="Regenerate the Elementary data observability report.")
def elementary_refresh_job():
    elementary_generate_report()
