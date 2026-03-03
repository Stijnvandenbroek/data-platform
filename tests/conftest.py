"""Shared test fixtures."""

from unittest.mock import MagicMock


def make_mock_engine(select_rows: list[tuple] | None = None):
    """Return a mock SQLAlchemy engine.

    Args:
        select_rows: Rows to return from *connect()* (SELECT queries).
            Defaults to an empty list.
    """
    select_rows = select_rows or []

    engine = MagicMock()

    # engine.begin() context manager → conn for DDL / writes
    write_conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=write_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    # engine.connect() context manager → conn for SELECTs
    read_conn = MagicMock()
    read_conn.execute.return_value = iter(select_rows)
    engine.connect.return_value.__enter__ = MagicMock(return_value=read_conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    return engine, write_conn, read_conn


def make_mock_listing(data: dict):
    """Return a mock pyfunda Listing-like object."""
    listing = MagicMock()
    listing.to_dict.return_value = data
    return listing
