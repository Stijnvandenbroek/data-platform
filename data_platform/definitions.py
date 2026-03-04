from dagster import Definitions
from dagster_dbt import DbtCliResource

from data_platform.assets.dbt import DBT_PROJECT_DIR, dbt_project_assets
from data_platform.assets.ingestion.funda import (
    funda_listing_details,
    funda_price_history,
    funda_search_results,
)
from data_platform.resources import FundaResource, PostgresResource
from data_platform.schedules import funda_ingestion_job, funda_ingestion_schedule

defs = Definitions(
    assets=[
        dbt_project_assets,
        funda_search_results,
        funda_listing_details,
        funda_price_history,
    ],
    jobs=[funda_ingestion_job],
    schedules=[funda_ingestion_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
        "funda": FundaResource(),
        "postgres": PostgresResource(),
    },
)
