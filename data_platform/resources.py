"""Shared Dagster resources for the data platform."""

import os

from dagster import ConfigurableResource
from funda import Funda
from sqlalchemy import create_engine, text


class FundaResource(ConfigurableResource):
    """Wrapper around the pyfunda client."""

    timeout: int = 30

    def get_client(self) -> Funda:
        return Funda(timeout=self.timeout)


class PostgresResource(ConfigurableResource):
    """Lightweight Postgres resource for raw ingestion writes."""

    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    user: str = os.getenv("POSTGRES_USER", "")
    password: str = os.getenv("POSTGRES_PASSWORD", "")
    dbname: str = os.getenv("POSTGRES_DB", "")

    def get_engine(self):
        url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        return create_engine(url)

    def execute(self, statement: str, params: dict | None = None):
        engine = self.get_engine()
        with engine.begin() as conn:
            conn.execute(text(statement), params or {})

    def execute_many(self, statement: str, rows: list[dict]):
        engine = self.get_engine()
        with engine.begin() as conn:
            conn.execute(text(statement), rows)
