from dagster import Definitions
from dagster_dbt import DbtCliResource

from data_platform.assets.dbt import DBT_PROJECT_DIR, dbt_project_assets
from data_platform.assets.funda import (
    funda_listing_details,
    funda_price_history,
    funda_search_results,
)
from data_platform.resources import FundaResource, PostgresResource

# ---------------------------------------------------------------------------
# Definitions
# ---------------------------------------------------------------------------

defs = Definitions(
    assets=[
        dbt_project_assets,
        funda_search_results,
        funda_listing_details,
        funda_price_history,
    ],
    resources={
        "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
        "funda": FundaResource(),
        "postgres": PostgresResource(),
    },
)
