"""dbt assets for the data platform."""

from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt"

dbt_project = DbtProject(project_dir=str(DBT_PROJECT_DIR))

# Generate manifest locally outside Docker.
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_project_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Expose every dbt model as a Dagster asset."""
    yield from dbt.cli(["build"], context=context).stream()
