"""Dagster resources."""

from dagster import ConfigurableResource, EnvVar
from funda import Funda
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool


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
        return create_engine(url, poolclass=NullPool)

    def execute(self, statement: str, params: dict | None = None):
        engine = self.get_engine()
        with engine.begin() as conn:
            conn.execute(text(statement), params or {})

    def execute_many(self, statement: str, rows: list[dict]):
        engine = self.get_engine()
        with engine.begin() as conn:
            conn.execute(text(statement), rows)


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
