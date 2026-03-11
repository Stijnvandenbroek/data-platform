"""Tests for ELO rating and comparison assets."""

from unittest.mock import MagicMock, patch

from dagster import build_asset_context

from data_platform.assets.elo.elo import _ensure_schema, elo_comparisons, elo_ratings


class TestEnsureSchema:
    def test_executes_create_schema_sql(self):
        conn = MagicMock()
        with patch(
            "data_platform.assets.elo.elo.render_sql",
            return_value="CREATE SCHEMA IF NOT EXISTS elo",
        ):
            _ensure_schema(conn)
        conn.execute.assert_called_once()


class TestEloRatings:
    @patch(
        "data_platform.assets.elo.elo.render_sql",
        return_value="CREATE TABLE IF NOT EXISTS elo.ratings ()",
    )
    def test_creates_table_and_returns_metadata(self, mock_render):
        postgres = MagicMock()
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        postgres.get_engine.return_value = engine

        context = build_asset_context()
        result = elo_ratings(context, postgres)

        assert result.metadata["schema"].value == "elo"
        assert result.metadata["table"].value == "ratings"
        # Two calls: _ensure_schema + create table
        assert conn.execute.call_count == 2

    @patch("data_platform.assets.elo.elo.render_sql", return_value="SQL")
    def test_calls_get_engine(self, mock_render):
        postgres = MagicMock()
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        postgres.get_engine.return_value = engine

        context = build_asset_context()
        elo_ratings(context, postgres)
        postgres.get_engine.assert_called_once()


class TestEloComparisons:
    @patch(
        "data_platform.assets.elo.elo.render_sql",
        return_value="CREATE TABLE IF NOT EXISTS elo.comparisons ()",
    )
    def test_creates_table_and_returns_metadata(self, mock_render):
        postgres = MagicMock()
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        postgres.get_engine.return_value = engine

        context = build_asset_context()
        result = elo_comparisons(context, postgres)

        assert result.metadata["schema"].value == "elo"
        assert result.metadata["table"].value == "comparisons"
        assert conn.execute.call_count == 2

    @patch("data_platform.assets.elo.elo.render_sql", return_value="SQL")
    def test_calls_get_engine(self, mock_render):
        postgres = MagicMock()
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        postgres.get_engine.return_value = engine

        context = build_asset_context()
        elo_comparisons(context, postgres)
        postgres.get_engine.assert_called_once()
