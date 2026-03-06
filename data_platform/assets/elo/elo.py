"""ELO schema and table management assets.

Individual assets for the stateful ELO tables (ratings, comparisons) used
by the house-elo-ranking application.  The sample_listings table is
managed as a dbt model in marts.
"""

from pathlib import Path

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset
from sqlalchemy import text

from data_platform.helpers import render_sql
from data_platform.resources import PostgresResource

_SQL_DIR = Path(__file__).parent / "sql"
_SCHEMA = "elo"


def _ensure_schema(conn: object) -> None:
    """Create the ELO schema if it does not exist."""
    schema_sql = render_sql(_SQL_DIR, "create_schema.sql", schema=_SCHEMA)
    conn.execute(text(schema_sql))


@asset(
    deps=["elo_sample_listings"],
    group_name="elo",
    description="Creates the ELO ratings table that stores per-listing ELO scores.",
)
def elo_ratings(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    """Ensure the ELO ratings table exists (idempotent)."""
    table_sql = render_sql(_SQL_DIR, "create_ratings_table.sql", schema=_SCHEMA)

    engine = postgres.get_engine()
    with engine.begin() as conn:
        _ensure_schema(conn)
        conn.execute(text(table_sql))

    context.log.info("ELO ratings table ensured.")

    return MaterializeResult(
        metadata={
            "schema": MetadataValue.text(_SCHEMA),
            "table": MetadataValue.text("ratings"),
        }
    )


@asset(
    deps=["elo_sample_listings"],
    group_name="elo",
    description="Creates the ELO comparisons table that records pairwise match results.",
)
def elo_comparisons(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    """Ensure the ELO comparisons table exists (idempotent)."""
    table_sql = render_sql(_SQL_DIR, "create_comparisons_table.sql", schema=_SCHEMA)

    engine = postgres.get_engine()
    with engine.begin() as conn:
        _ensure_schema(conn)
        conn.execute(text(table_sql))

    context.log.info("ELO comparisons table ensured.")

    return MaterializeResult(
        metadata={
            "schema": MetadataValue.text(_SCHEMA),
            "table": MetadataValue.text("comparisons"),
        }
    )
