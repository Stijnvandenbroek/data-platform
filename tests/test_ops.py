"""Tests for Dagster ops — elementary and source freshness."""

import subprocess
from unittest.mock import patch

import pytest
from dagster import build_op_context

from data_platform.ops.check_source_freshness import (
    SourceFreshnessConfig,
)
from data_platform.ops.elementary import (
    _elementary_schema_exists,
    elementary_generate_report,
    elementary_run_models,
)


# ---------------------------------------------------------------------------
# SourceFreshnessConfig
# ---------------------------------------------------------------------------


class TestSourceFreshnessConfig:
    def test_accepts_source_name(self):
        cfg = SourceFreshnessConfig(source_name="raw_funda")
        assert cfg.source_name == "raw_funda"


# ---------------------------------------------------------------------------
# _elementary_schema_exists
# ---------------------------------------------------------------------------


class TestElementarySchemaExists:
    @patch("data_platform.resources._retry_on_operational_error")
    @patch("data_platform.ops.elementary.create_engine")
    def test_returns_true_when_schema_present(self, mock_create_engine, mock_retry):
        mock_retry.return_value = True
        with patch.dict(
            "os.environ",
            {
                "POSTGRES_USER": "u",
                "POSTGRES_PASSWORD": "p",
                "POSTGRES_HOST": "localhost",
                "POSTGRES_PORT": "5432",
                "POSTGRES_DB": "db",
            },
        ):
            result = _elementary_schema_exists()
        assert result is True
        mock_create_engine.assert_called_once()

    @patch("data_platform.resources._retry_on_operational_error")
    @patch("data_platform.ops.elementary.create_engine")
    def test_returns_false_when_schema_absent(self, mock_create_engine, mock_retry):
        mock_retry.return_value = False
        with patch.dict(
            "os.environ",
            {
                "POSTGRES_USER": "u",
                "POSTGRES_PASSWORD": "p",
                "POSTGRES_HOST": "localhost",
                "POSTGRES_PORT": "5432",
                "POSTGRES_DB": "db",
            },
        ):
            result = _elementary_schema_exists()
        assert result is False


# ---------------------------------------------------------------------------
# elementary_run_models
# ---------------------------------------------------------------------------


class TestElementaryRunModels:
    @patch("data_platform.ops.elementary._elementary_schema_exists", return_value=True)
    def test_skips_when_schema_exists(self, mock_exists):
        context = build_op_context()
        elementary_run_models(context)
        mock_exists.assert_called_once()

    @patch(
        "data_platform.ops.elementary.subprocess.run",
        return_value=subprocess.CompletedProcess(
            args=[], returncode=0, stdout="ok", stderr=""
        ),
    )
    @patch("data_platform.ops.elementary._elementary_schema_exists", return_value=False)
    def test_runs_dbt_when_schema_missing(self, mock_exists, mock_run):
        context = build_op_context()
        elementary_run_models(context)
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert "dbt" in args
        assert "run" in args
        assert "elementary" in args

    @patch(
        "data_platform.ops.elementary.subprocess.run",
        return_value=subprocess.CompletedProcess(
            args=[], returncode=1, stdout="", stderr="error"
        ),
    )
    @patch("data_platform.ops.elementary._elementary_schema_exists", return_value=False)
    def test_raises_on_dbt_failure(self, mock_exists, mock_run):
        context = build_op_context()
        with pytest.raises(Exception, match="dbt run elementary failed"):
            elementary_run_models(context)


# ---------------------------------------------------------------------------
# elementary_generate_report
# ---------------------------------------------------------------------------


class TestElementaryGenerateReport:
    @patch(
        "data_platform.ops.elementary.subprocess.run",
        return_value=subprocess.CompletedProcess(
            args=[], returncode=0, stdout="report generated", stderr=""
        ),
    )
    def test_calls_edr_report(self, mock_run):
        context = build_op_context()
        elementary_generate_report(context)
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert "edr" in args
        assert "report" in args

    @patch(
        "data_platform.ops.elementary.subprocess.run",
        return_value=subprocess.CompletedProcess(
            args=[], returncode=1, stdout="", stderr="fatal error"
        ),
    )
    def test_raises_on_failure(self, mock_run):
        context = build_op_context()
        with pytest.raises(Exception, match="edr report failed"):
            elementary_generate_report(context)

    @patch(
        "data_platform.ops.elementary.subprocess.run",
        return_value=subprocess.CompletedProcess(
            args=[], returncode=0, stdout="done", stderr=""
        ),
    )
    def test_success_returns_none(self, mock_run):
        context = build_op_context()
        result = elementary_generate_report(context)
        assert result is None
