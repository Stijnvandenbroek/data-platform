"""Elementary schedules."""

from dagster import DefaultScheduleStatus, ScheduleDefinition

from data_platform.jobs.elementary import elementary_refresh_job

elementary_refresh_schedule = ScheduleDefinition(
    name="elementary_refresh_schedule",
    job=elementary_refresh_job,
    cron_schedule="0 9 * * *",
    description="Regenerate the Elementary report daily at 09:00 UTC.",
    default_status=DefaultScheduleStatus.RUNNING,
)
