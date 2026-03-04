"""dbt source freshness op."""

from dagster import Config, OpExecutionContext, op
from dagster_dbt import DbtCliResource


class SourceFreshnessConfig(Config):
    """Config for the source freshness op."""

    source_name: str


@op
def check_source_freshness(
    context: OpExecutionContext,
    config: SourceFreshnessConfig,
    dbt: DbtCliResource,
) -> None:
    """Run dbt source freshness for the configured source."""
    list(
        dbt.cli(
            ["source", "freshness", "--select", f"source:{config.source_name}"],
            context=context,
        ).stream()
    )
