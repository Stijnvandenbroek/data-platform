"""Dagster jobs and schedules for the data platform."""

from dagster import (
    AssetSelection,
    RunConfig,
    ScheduleDefinition,
    define_asset_job,
)

from data_platform.assets.funda import (
    FundaDetailsConfig,
    FundaPriceHistoryConfig,
    FundaSearchConfig,
)

# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

funda_ingestion_job = define_asset_job(
    name="funda_ingestion",
    selection=AssetSelection.assets(
        "funda_search_results",
        "funda_listing_details",
        "funda_price_history",
    ),
    description="Run the full Funda ingestion pipeline (search → details → price history).",
)

# ---------------------------------------------------------------------------
# Schedules
# ---------------------------------------------------------------------------

funda_ingestion_schedule = ScheduleDefinition(
    name="funda_ingestion_schedule",
    job=funda_ingestion_job,
    cron_schedule="0 */4 * * *",  # every 4 hours
    run_config=RunConfig(
        ops={
            "funda_search_results": FundaSearchConfig(),
            "funda_listing_details": FundaDetailsConfig(),
            "funda_price_history": FundaPriceHistoryConfig(),
        }
    ),
    default_status="RUNNING",
)
