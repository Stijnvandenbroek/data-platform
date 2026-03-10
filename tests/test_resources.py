"""Tests for data_platform.resources."""

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from data_platform.resources import (
    DiscordResource,
    FundaResource,
    MLflowResource,
    PostgresResource,
    _retry_on_operational_error,
)


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

    def test_engine_uses_pool_pre_ping(self):
        res = self._make_resource()
        with patch("data_platform.resources.create_engine") as mock_create:
            mock_create.return_value = MagicMock()
            res.get_engine()
            kwargs = mock_create.call_args[1]
            assert kwargs["pool_pre_ping"] is True

    def test_engine_sets_connect_timeout(self):
        res = self._make_resource()
        with patch("data_platform.resources.create_engine") as mock_create:
            mock_create.return_value = MagicMock()
            res.get_engine()
            kwargs = mock_create.call_args[1]
            assert kwargs["connect_args"]["connect_timeout"] == 10

    def test_execute_retries_on_operational_error(self):
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = [
            OperationalError("conn", {}, Exception("DNS failure")),
            None,
        ]
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        with (
            patch("data_platform.resources.create_engine", return_value=mock_engine),
            patch("data_platform.resources.time.sleep"),
        ):
            res = self._make_resource()
            res.execute("SELECT 1")

    def test_execute_calls_engine_begin(self):
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

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


class TestRetryOnOperationalError:
    def test_succeeds_on_first_attempt(self):
        fn = MagicMock(return_value="ok")
        result = _retry_on_operational_error(fn, attempts=3, base_delay=0)
        assert result == "ok"
        assert fn.call_count == 1

    @patch("data_platform.resources.time.sleep")
    def test_retries_then_succeeds(self, mock_sleep):
        fn = MagicMock(
            side_effect=[
                OperationalError("conn", {}, Exception("DNS failure")),
                "ok",
            ]
        )
        result = _retry_on_operational_error(fn, attempts=3, base_delay=1)
        assert result == "ok"
        assert fn.call_count == 2
        mock_sleep.assert_called_once_with(1)

    @patch("data_platform.resources.time.sleep")
    def test_raises_after_all_attempts_exhausted(self, mock_sleep):
        fn = MagicMock(
            side_effect=OperationalError("conn", {}, Exception("DNS failure"))
        )
        with pytest.raises(OperationalError):
            _retry_on_operational_error(fn, attempts=3, base_delay=1)
        assert fn.call_count == 3

    @patch("data_platform.resources.time.sleep")
    def test_exponential_backoff(self, mock_sleep):
        fn = MagicMock(
            side_effect=[
                OperationalError("conn", {}, Exception("DNS failure")),
                OperationalError("conn", {}, Exception("DNS failure")),
                "ok",
            ]
        )
        _retry_on_operational_error(fn, attempts=5, base_delay=1)
        assert mock_sleep.call_args_list == [
            ((1,),),
            ((2,),),
        ]


class TestMLflowResource:
    def test_tracking_uri(self):
        resource = MLflowResource(tracking_uri="http://mlflow:5000")
        assert resource.get_tracking_uri() == "http://mlflow:5000"


class TestDiscordResource:
    def test_webhook_url(self):
        resource = DiscordResource(webhook_url="https://discord.com/api/webhooks/test")
        assert resource.get_webhook_url() == "https://discord.com/api/webhooks/test"
