"""Funda jobs."""

from dagster import AssetSelection, RunConfig, define_asset_job, job

from data_platform.ops.check_source_freshness import (
    SourceFreshnessConfig,
    check_source_freshness,
)

funda_ingestion_job = define_asset_job(
    name="funda_ingestion",
    selection=AssetSelection.assets(
        "funda_search_results",
        "funda_listing_details",
        "funda_price_history",
    ),
    description="Full Funda ingestion pipeline.",
)


@job(
    description="dbt source freshness checks for all raw_funda sources.",
    config=RunConfig(
        ops={"check_source_freshness": SourceFreshnessConfig(source_name="raw_funda")}
    ),
)
def funda_raw_quality_job():
    check_source_freshness()
