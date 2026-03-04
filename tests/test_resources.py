"""Tests for data_platform.resources."""

from unittest.mock import MagicMock, patch

from data_platform.resources import FundaResource, PostgresResource


class TestFundaResource:
    def test_get_client_returns_funda_instance(self):
        resource = FundaResource(timeout=10)
        from funda import Funda

        client = resource.get_client()
        assert isinstance(client, Funda)

    def test_default_timeout(self):
        resource = FundaResource()
        assert resource.timeout == 30

    def test_custom_timeout(self):
        resource = FundaResource(timeout=60)
        assert resource.timeout == 60


class TestPostgresResource:
    def _make_resource(self, **kwargs):
        defaults = {
            "host": "testhost",
            "port": 5432,
            "user": "user",
            "password": "pw",
            "dbname": "db",
        }
        return PostgresResource(**{**defaults, **kwargs})

    def test_connection_url_format(self):
        res = self._make_resource()
        # Patch at the module level so the frozen instance isn't mutated
        with patch("data_platform.resources.create_engine") as mock_create:
            mock_create.return_value = MagicMock()
            res.get_engine()
            call_url = mock_create.call_args[0][0]
            assert "testhost" in call_url
            assert "5432" in call_url
            assert "user" in call_url
            assert "pw" in call_url
            assert "db" in call_url

    def test_connection_url_scheme(self):
        res = self._make_resource()
        with patch("data_platform.resources.create_engine") as mock_create:
            mock_create.return_value = MagicMock()
            res.get_engine()
            call_url = mock_create.call_args[0][0]
            assert call_url.startswith("postgresql://")

    def test_execute_calls_engine_begin(self):
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        # Patch create_engine at module level so that get_engine() returns our mock
        with patch("data_platform.resources.create_engine", return_value=mock_engine):
            res = self._make_resource()
            res.execute("SELECT 1")

        mock_conn.execute.assert_called_once()

    def test_execute_many_calls_engine_begin(self):
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        with patch("data_platform.resources.create_engine", return_value=mock_engine):
            res = self._make_resource()
            rows = [{"id": 1}, {"id": 2}]
            res.execute_many("INSERT INTO t VALUES (:id)", rows)

        mock_conn.execute.assert_called_once()
