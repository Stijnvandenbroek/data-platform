"""Tests for pure helper functions in data_platform.assets.helpers."""

from data_platform.assets.helpers import (
    format_area,
    format_euro,
    md_preview_table,
    safe,
    safe_int,
)

# ── safe_int ────────────────────────────────────────────────────────────────


class TestSafeInt:
    def test_none_returns_none(self):
        assert safe_int(None) is None

    def test_integer_passthrough(self):
        assert safe_int(42) == 42

    def test_negative_integer(self):
        assert safe_int(-10) == -10

    def test_zero(self):
        assert safe_int(0) == 0

    def test_string_int(self):
        assert safe_int("123") == 123

    def test_float_truncated(self):
        assert safe_int(3.9) == 3

    def test_float_string(self):
        assert safe_int("7.0") == 7

    def test_non_numeric_string_returns_none(self):
        assert safe_int("abc") is None

    def test_empty_string_returns_none(self):
        assert safe_int("") is None

    def test_list_returns_none(self):
        assert safe_int([1, 2, 3]) is None


# ── safe ─────────────────────────────────────────────────────────────────────


class TestSafe:
    def test_dict_becomes_json_string(self):
        result = safe({"key": "val"})
        assert result == '{"key": "val"}'

    def test_list_becomes_json_string(self):
        result = safe([1, 2, 3])
        assert result == "[1, 2, 3]"

    def test_tuple_becomes_json_string(self):
        result = safe((1, 2))
        assert result == "[1, 2]"

    def test_string_passthrough(self):
        assert safe("hello") == "hello"

    def test_integer_passthrough(self):
        assert safe(99) == 99

    def test_none_passthrough(self):
        assert safe(None) is None

    def test_nested_dict_serialised(self):
        data = {"a": {"b": [1, 2]}}
        result = safe(data)
        import json

        assert json.loads(result) == data


# ── format_euro ──────────────────────────────────────────────────────────────


class TestFormatEuro:
    def test_formats_price(self):
        assert format_euro(350000) == "€350,000"

    def test_none_returns_dash(self):
        assert format_euro(None) == "–"

    def test_zero_returns_dash(self):
        assert format_euro(0) == "–"


# ── format_area ──────────────────────────────────────────────────────────────


class TestFormatArea:
    def test_formats_area(self):
        assert format_area(80) == "80 m²"

    def test_none_returns_dash(self):
        assert format_area(None) == "–"

    def test_zero_returns_dash(self):
        assert format_area(0) == "–"


# ── md_preview_table ─────────────────────────────────────────────────────────


class TestMdPreviewTable:
    def test_empty_rows_returns_header_only(self):
        result = md_preview_table([], columns=[("title", "Title"), ("city", "City")])
        lines = result.split("\n")
        assert len(lines) == 2
        assert "Title" in lines[0]
        assert "---" in lines[1]

    def test_single_row_appears(self):
        rows = [{"title": "Teststraat 1", "city": "Amsterdam", "price": 350000}]
        result = md_preview_table(
            rows,
            columns=[("title", "Title"), ("city", "City"), ("price", "Price")],
            formatters={"price": format_euro},
        )
        assert "Teststraat 1" in result
        assert "Amsterdam" in result
        assert "€350,000" in result

    def test_missing_value_shows_dash(self):
        rows = [{"title": "No Price", "city": "Rotterdam"}]
        result = md_preview_table(
            rows,
            columns=[("title", "Title"), ("city", "City"), ("price", "Price")],
            formatters={"price": format_euro},
        )
        assert "–" in result

    def test_multiple_rows_correct_count(self):
        rows = [{"title": f"St {i}", "city": "City"} for i in range(5)]
        result = md_preview_table(rows, columns=[("title", "Title"), ("city", "City")])
        lines = result.split("\n")
        # header + separator + 5 data rows
        assert len(lines) == 7
