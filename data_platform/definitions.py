from dagster import Definitions
from dagster_dbt import DbtCliResource

from data_platform.assets.dbt import DBT_PROJECT_DIR, dbt_project_assets
from data_platform.assets.ingestion.funda import (
    raw_funda_listing_details,
    raw_funda_price_history,
    raw_funda_search_results,
)
from data_platform.helpers import apply_automation
from data_platform.jobs import (
    elementary_refresh_job,
    funda_ingestion_job,
    funda_raw_quality_job,
)
from data_platform.resources import FundaResource, PostgresResource
from data_platform.schedules import (
    elementary_refresh_schedule,
    funda_ingestion_schedule,
    funda_raw_quality_schedule,
)

defs = Definitions(
    assets=apply_automation(
        [
            dbt_project_assets,
            raw_funda_search_results,
            raw_funda_listing_details,
            raw_funda_price_history,
        ]
    ),
    jobs=[funda_ingestion_job, funda_raw_quality_job, elementary_refresh_job],
    schedules=[
        funda_ingestion_schedule,
        funda_raw_quality_schedule,
        elementary_refresh_schedule,
    ],
    resources={
        "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
        "funda": FundaResource(),
        "postgres": PostgresResource(),
    },
)
