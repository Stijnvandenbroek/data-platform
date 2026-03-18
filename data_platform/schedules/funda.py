"""Funda schedules."""

from dagster import DefaultScheduleStatus, RunConfig, ScheduleDefinition

from data_platform.assets.ingestion.funda.funda import (
    FundaDetailsConfig,
    FundaPriceHistoryConfig,
    FundaSearchConfig,
)
from data_platform.jobs.funda import funda_ingestion_job, funda_raw_quality_job

funda_ingestion_schedule = ScheduleDefinition(
    name="funda_ingestion_schedule",
    job=funda_ingestion_job,
    cron_schedule="0 * * * *",
    run_config=RunConfig(
        ops={
            "raw_funda_search_results": FundaSearchConfig(),
            "raw_funda_listing_details": FundaDetailsConfig(),
            "raw_funda_price_history": FundaPriceHistoryConfig(),
        }
    ),
    default_status=DefaultScheduleStatus.RUNNING,
)

funda_raw_quality_schedule = ScheduleDefinition(
    name="funda_raw_quality_schedule",
    job=funda_raw_quality_job,
    cron_schedule="0 8 * * *",
    description="Daily quality checks on all raw Funda tables.",
    default_status=DefaultScheduleStatus.RUNNING,
)
