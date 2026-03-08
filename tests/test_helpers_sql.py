"""Tests for data_platform.helpers.sql — render_sql."""

from pathlib import Path

import pytest
from jinja2 import TemplateNotFound

from data_platform.helpers import render_sql


_FIXTURES = Path(__file__).parent / "fixtures" / "sql"


class TestRenderSql:
    def test_renders_plain_sql(self, tmp_path):
        sql_file = tmp_path / "plain.sql"
        sql_file.write_text("select 1")
        result = render_sql(tmp_path, "plain.sql")
        assert result == "select 1"

    def test_renders_with_variables(self, tmp_path):
        sql_file = tmp_path / "schema.sql"
        sql_file.write_text("create schema if not exists {{ schema }}")
        result = render_sql(tmp_path, "schema.sql", schema="elo")
        assert result == "create schema if not exists elo"

    def test_missing_template_raises(self, tmp_path):
        with pytest.raises(TemplateNotFound):
            render_sql(tmp_path, "nonexistent.sql")

    def test_multiple_variables(self, tmp_path):
        sql_file = tmp_path / "multi.sql"
        sql_file.write_text("select * from {{ schema }}.{{ table }}")
        result = render_sql(tmp_path, "multi.sql", schema="raw", table="events")
        assert result == "select * from raw.events"
