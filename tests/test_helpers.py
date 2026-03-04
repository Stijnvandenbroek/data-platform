"""Tests for pure helper functions in data_platform.assets.funda."""

from data_platform.assets.funda import (
    _details_preview_table,
    _safe,
    _safe_int,
    _search_preview_table,
)

# ── _safe_int ───────────────────────────────────────────────────────────────


class TestSafeInt:
    def test_none_returns_none(self):
        assert _safe_int(None) is None

    def test_integer_passthrough(self):
        assert _safe_int(42) == 42

    def test_negative_integer(self):
        assert _safe_int(-10) == -10

    def test_zero(self):
        assert _safe_int(0) == 0

    def test_string_int(self):
        assert _safe_int("123") == 123

    def test_float_truncated(self):
        assert _safe_int(3.9) == 3

    def test_float_string(self):
        assert _safe_int("7.0") == 7

    def test_non_numeric_string_returns_none(self):
        assert _safe_int("abc") is None

    def test_empty_string_returns_none(self):
        assert _safe_int("") is None

    def test_list_returns_none(self):
        assert _safe_int([1, 2, 3]) is None


# ── _safe ────────────────────────────────────────────────────────────────────


class TestSafe:
    def test_dict_becomes_json_string(self):
        result = _safe({"key": "val"})
        assert result == '{"key": "val"}'

    def test_list_becomes_json_string(self):
        result = _safe([1, 2, 3])
        assert result == "[1, 2, 3]"

    def test_tuple_becomes_json_string(self):
        result = _safe((1, 2))
        assert result == "[1, 2]"

    def test_string_passthrough(self):
        assert _safe("hello") == "hello"

    def test_integer_passthrough(self):
        assert _safe(99) == 99

    def test_none_passthrough(self):
        assert _safe(None) is None

    def test_nested_dict_serialised(self):
        data = {"a": {"b": [1, 2]}}
        result = _safe(data)
        import json

        assert json.loads(result) == data


# ── _search_preview_table ────────────────────────────────────────────────────


class TestSearchPreviewTable:
    def test_empty_rows_returns_header_only(self):
        result = _search_preview_table([])
        lines = result.split("\n")
        assert len(lines) == 2
        assert "Title" in lines[0]
        assert "---" in lines[1]

    def test_single_row_appears(self):
        rows = [
            {
                "title": "Teststraat 1",
                "city": "Amsterdam",
                "price": 350000,
                "living_area": 80,
                "bedrooms": 3,
            }
        ]
        result = _search_preview_table(rows)
        assert "Teststraat 1" in result
        assert "Amsterdam" in result
        assert "€350,000" in result
        assert "80 m²" in result
        assert "3" in result

    def test_missing_price_shows_dash(self):
        rows = [{"title": "No Price", "city": "Rotterdam", "price": None}]
        result = _search_preview_table(rows)
        assert "–" in result

    def test_missing_area_shows_dash(self):
        rows = [{"title": "No Area", "city": "Utrecht", "living_area": None}]
        result = _search_preview_table(rows)
        assert "–" in result

    def test_multiple_rows_correct_count(self):
        rows = [
            {"title": f"St {i}", "city": "City", "price": i * 1000} for i in range(5)
        ]
        result = _search_preview_table(rows)
        lines = result.split("\n")
        # header + separator + 5 data rows
        assert len(lines) == 7


# ── _details_preview_table ───────────────────────────────────────────────────


class TestDetailsPreviewTable:
    def test_empty_rows_returns_header_only(self):
        result = _details_preview_table([])
        lines = result.split("\n")
        assert len(lines) == 2
        assert "Title" in lines[0]

    def test_row_with_all_fields(self):
        rows = [
            {
                "title": "Kerkstraat 5",
                "city": "Haarlem",
                "price": 425000,
                "status": "available",
                "energy_label": "A",
            }
        ]
        result = _details_preview_table(rows)
        assert "Kerkstraat 5" in result
        assert "Haarlem" in result
        assert "€425,000" in result
        assert "available" in result
        assert "A" in result

    def test_missing_price_shows_dash(self):
        rows = [{"title": "T", "city": "C", "price": None, "status": "sold"}]
        result = _details_preview_table(rows)
        assert "–" in result
