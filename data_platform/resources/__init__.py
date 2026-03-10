"""Dagster resources."""

import time

from dagster import ConfigurableResource, EnvVar, get_dagster_logger
from funda import Funda
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

logger = get_dagster_logger()

_RETRY_ATTEMPTS = 5
_RETRY_BASE_DELAY = 1  # seconds; doubles each attempt


def _retry_on_operational_error(
    fn, *, attempts=_RETRY_ATTEMPTS, base_delay=_RETRY_BASE_DELAY
):
    """Retry *fn* with exponential back-off on SQLAlchemy OperationalError."""
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except OperationalError:
            if attempt == attempts:
                raise
            delay = base_delay * 2 ** (attempt - 1)
            logger.warning(
                "DB connection attempt %d/%d failed, retrying in %ds …",
                attempt,
                attempts,
                delay,
            )
            time.sleep(delay)


class FundaResource(ConfigurableResource):
    """Wrapper around the pyfunda client."""

    timeout: int = 30

    def get_client(self) -> Funda:
        return Funda(timeout=self.timeout)


class PostgresResource(ConfigurableResource):
    """Lightweight Postgres resource for raw ingestion writes."""

    host: str = EnvVar("POSTGRES_HOST")
    port: int = EnvVar.int("POSTGRES_PORT")
    user: str = EnvVar("POSTGRES_USER")
    password: str = EnvVar("POSTGRES_PASSWORD")
    dbname: str = EnvVar("POSTGRES_DB")

    def get_engine(self):
        url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        return create_engine(
            url,
            pool_pre_ping=True,
            pool_size=2,
            max_overflow=3,
            connect_args={"connect_timeout": 10},
        )

    def execute(self, statement: str, params: dict | None = None):
        engine = self.get_engine()

        def _run():
            with engine.begin() as conn:
                conn.execute(text(statement), params or {})

        _retry_on_operational_error(_run)

    def execute_many(self, statement: str, rows: list[dict]):
        engine = self.get_engine()

        def _run():
            with engine.begin() as conn:
                conn.execute(text(statement), rows)

        _retry_on_operational_error(_run)


class MLflowResource(ConfigurableResource):
    """MLflow experiment tracking resource."""

    tracking_uri: str = EnvVar("MLFLOW_TRACKING_URI")

    def get_tracking_uri(self) -> str:
        return self.tracking_uri


class DiscordResource(ConfigurableResource):
    """Discord webhook resource for sending notifications."""

    webhook_url: str = EnvVar("DISCORD_WEBHOOK_URL")

    def get_webhook_url(self) -> str:
        return self.webhook_url
