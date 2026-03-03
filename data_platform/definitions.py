from pathlib import Path

from dagster import Definitions
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from data_platform.assets.funda import (
    funda_listing_details,
    funda_price_history,
    funda_search_results,
)
from data_platform.resources import FundaResource, PostgresResource

# ---------------------------------------------------------------------------
# dbt project
# ---------------------------------------------------------------------------

DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt"

dbt_project = DbtProject(project_dir=str(DBT_PROJECT_DIR))

# When running locally outside Docker, generate/refresh the manifest automatically.
dbt_project.prepare_if_dev()


# ---------------------------------------------------------------------------
# dbt assets – every dbt model/test/snapshot becomes a Dagster asset
# ---------------------------------------------------------------------------


@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_project_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


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
