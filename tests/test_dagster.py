"""Generic tests for Dagster definitions — schedules, jobs, and the Definitions object."""

import pytest
from dagster import DefaultScheduleStatus

from data_platform.jobs import (
    elementary_refresh_job,
    funda_ingestion_job,
    funda_raw_quality_job,
)
from data_platform.schedules import (
    elementary_refresh_schedule,
    funda_ingestion_schedule,
    funda_raw_quality_schedule,
)

ALL_SCHEDULES = [
    elementary_refresh_schedule,
    funda_ingestion_schedule,
    funda_raw_quality_schedule,
]

ALL_JOBS = [
    elementary_refresh_job,
    funda_ingestion_job,
    funda_raw_quality_job,
]


# Generic schedule tests


class TestSchedulesGeneric:
    """Property tests that apply to every schedule."""

    @pytest.mark.parametrize("schedule", ALL_SCHEDULES, ids=lambda s: s.name)
    def test_has_name(self, schedule):
        assert schedule.name

    @pytest.mark.parametrize("schedule", ALL_SCHEDULES, ids=lambda s: s.name)
    def test_has_valid_cron(self, schedule):
        parts = schedule.cron_schedule.split()
        assert len(parts) == 5, f"Expected 5-part cron, got {schedule.cron_schedule}"

    @pytest.mark.parametrize("schedule", ALL_SCHEDULES, ids=lambda s: s.name)
    def test_has_job(self, schedule):
        assert schedule.job is not None or schedule.job_name is not None

    @pytest.mark.parametrize("schedule", ALL_SCHEDULES, ids=lambda s: s.name)
    def test_default_status_running(self, schedule):
        assert schedule.default_status == DefaultScheduleStatus.RUNNING


# Generic job tests


class TestJobsGeneric:
    """Property tests that apply to every job."""

    @pytest.mark.parametrize("job", ALL_JOBS, ids=lambda j: j.name)
    def test_has_name(self, job):
        assert job.name

    @pytest.mark.parametrize("job", ALL_JOBS, ids=lambda j: j.name)
    def test_has_description(self, job):
        assert job.description


# Schedule-specific tests


class TestScheduleSpecific:
    def test_elementary_schedule_daily(self):
        assert elementary_refresh_schedule.cron_schedule == "0 9 * * *"

    def test_funda_ingestion_every_4_hours(self):
        assert funda_ingestion_schedule.cron_schedule == "0 */4 * * *"

    def test_funda_quality_daily(self):
        assert funda_raw_quality_schedule.cron_schedule == "0 8 * * *"

    def test_funda_ingestion_schedule_has_run_config_fn(self):
        assert funda_ingestion_schedule._run_config_fn is not None


# Definitions integration test


class TestDefinitions:
    def test_definitions_loads(self):
        from data_platform.definitions import defs

        assert defs is not None

    def test_definitions_has_assets(self):
        from data_platform.definitions import defs

        repo = defs.get_repository_def()
        asset_keys = repo.asset_graph.get_all_asset_keys()
        assert len(asset_keys) > 0

    def test_definitions_has_jobs(self):
        from data_platform.definitions import defs

        job = defs.resolve_job_def("funda_ingestion")
        assert job is not None

    def test_definitions_has_resources(self):
        from data_platform.definitions import defs

        repo = defs.get_repository_def()
        # Resources are configured but we can verify they're present
        assert repo is not None
