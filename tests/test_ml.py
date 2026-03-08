"""Tests for ML helper functions — preprocess, _best_run, _build_embed."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from data_platform.assets.ml.elo_model import (
    ALL_FEATURES,
    ENERGY_LABEL_MAP,
    preprocess,
)
from data_platform.assets.ml.discord_alerts import _build_embed
from data_platform.assets.ml.elo_inference import _best_run


# ---------------------------------------------------------------------------
# preprocess
# ---------------------------------------------------------------------------


class TestPreprocess:
    def _make_df(self, overrides: dict | None = None) -> pd.DataFrame:
        """Build a single-row DataFrame with sensible defaults."""
        data = {
            "energy_label": "A",
            "current_price": 350_000,
            "living_area": 80,
            "plot_area": 120,
            "bedrooms": 3,
            "rooms": 5,
            "construction_year": 2000,
            "latitude": 52.0,
            "longitude": 4.5,
            "photo_count": 10,
            "views": 100,
            "saves": 20,
            "price_per_sqm": 4375,
            "has_garden": True,
            "has_balcony": False,
            "has_solar_panels": None,
            "has_heat_pump": False,
            "has_roof_terrace": None,
            "is_energy_efficient": True,
            "is_monument": False,
        }
        if overrides:
            data.update(overrides)
        return pd.DataFrame([data])

    def test_energy_label_mapped(self):
        df = preprocess(self._make_df({"energy_label": "A"}))
        assert df["energy_label_num"].iloc[0] == ENERGY_LABEL_MAP["A"]

    def test_energy_label_g(self):
        df = preprocess(self._make_df({"energy_label": "G"}))
        assert df["energy_label_num"].iloc[0] == -1

    def test_energy_label_unknown(self):
        df = preprocess(self._make_df({"energy_label": "Z"}))
        assert df["energy_label_num"].iloc[0] == -2

    def test_energy_label_case_insensitive(self):
        df = preprocess(self._make_df({"energy_label": " a "}))
        assert df["energy_label_num"].iloc[0] == ENERGY_LABEL_MAP["A"]

    def test_bool_none_becomes_zero(self):
        df = preprocess(self._make_df({"has_garden": None}))
        assert df["has_garden"].iloc[0] == 0

    def test_bool_true_becomes_one(self):
        df = preprocess(self._make_df({"has_garden": True}))
        assert df["has_garden"].iloc[0] == 1

    def test_bool_false_becomes_zero(self):
        df = preprocess(self._make_df({"has_garden": False}))
        assert df["has_garden"].iloc[0] == 0

    def test_numeric_string_coerced(self):
        df = preprocess(self._make_df({"current_price": "350000"}))
        assert df["current_price"].iloc[0] == 350_000.0

    def test_numeric_nan_filled_with_median(self):
        data = self._make_df()
        row2 = self._make_df({"current_price": None})
        df = pd.concat([data, row2], ignore_index=True)
        df = preprocess(df)
        # median of [350000] is 350000 (single non-null value)
        assert df["current_price"].iloc[1] == 350_000.0

    def test_all_features_present(self):
        df = preprocess(self._make_df())
        for feat in ALL_FEATURES:
            assert feat in df.columns, f"Missing feature: {feat}"


# ---------------------------------------------------------------------------
# _best_run
# ---------------------------------------------------------------------------


class TestBestRun:
    @patch("data_platform.assets.ml.elo_inference.mlflow")
    def test_no_experiment_raises(self, mock_mlflow):
        mock_mlflow.tracking.MlflowClient.return_value.get_experiment_by_name.return_value = None
        with pytest.raises(ValueError, match="does not exist"):
            _best_run("nonexistent", "rmse", ascending=True)

    @patch("data_platform.assets.ml.elo_inference.mlflow")
    def test_no_runs_raises(self, mock_mlflow):
        client = mock_mlflow.tracking.MlflowClient.return_value
        client.get_experiment_by_name.return_value = MagicMock(experiment_id="1")
        client.search_runs.return_value = []
        with pytest.raises(ValueError, match="No runs found"):
            _best_run("experiment", "rmse", ascending=True)

    @patch("data_platform.assets.ml.elo_inference.mlflow")
    def test_ascending_order(self, mock_mlflow):
        client = mock_mlflow.tracking.MlflowClient.return_value
        client.get_experiment_by_name.return_value = MagicMock(experiment_id="1")
        run = MagicMock()
        client.search_runs.return_value = [run]

        result = _best_run("experiment", "rmse", ascending=True)
        assert result is run
        client.search_runs.assert_called_once_with(
            experiment_ids=["1"],
            order_by=["metrics.rmse ASC"],
            max_results=1,
        )

    @patch("data_platform.assets.ml.elo_inference.mlflow")
    def test_descending_order(self, mock_mlflow):
        client = mock_mlflow.tracking.MlflowClient.return_value
        client.get_experiment_by_name.return_value = MagicMock(experiment_id="1")
        run = MagicMock()
        client.search_runs.return_value = [run]

        result = _best_run("experiment", "r2", ascending=False)
        assert result is run
        client.search_runs.assert_called_once_with(
            experiment_ids=["1"],
            order_by=["metrics.r2 DESC"],
            max_results=1,
        )


# ---------------------------------------------------------------------------
# _build_embed
# ---------------------------------------------------------------------------


class TestBuildEmbed:
    def _make_row(self, **overrides):
        data = {
            "predicted_elo": 1650.0,
            "current_price": 350_000,
            "city": "Amsterdam",
            "living_area": 80,
            "rooms": 4,
            "energy_label": "A",
            "price_per_sqm": 4375,
            "title": "Teststraat 1",
            "global_id": "abc123",
            "url": "https://funda.nl/abc123",
        }
        data.update(overrides)
        return SimpleNamespace(**data)

    def test_all_fields_present(self):
        embed = _build_embed(self._make_row())
        field_names = [f["name"] for f in embed["fields"]]
        assert "Predicted ELO" in field_names
        assert "Price" in field_names
        assert "City" in field_names
        assert "Living area" in field_names
        assert "Rooms" in field_names
        assert "Energy label" in field_names
        assert "€/m²" in field_names

    def test_no_price_per_sqm_omits_field(self):
        embed = _build_embed(self._make_row(price_per_sqm=None))
        field_names = [f["name"] for f in embed["fields"]]
        assert "€/m²" not in field_names
        assert len(embed["fields"]) == 6

    def test_missing_city_shows_dash(self):
        embed = _build_embed(self._make_row(city=None))
        city_field = next(f for f in embed["fields"] if f["name"] == "City")
        assert city_field["value"] == "–"

    def test_title_fallback_to_global_id(self):
        embed = _build_embed(self._make_row(title=None))
        assert embed["title"] == "abc123"

    def test_url_set(self):
        embed = _build_embed(self._make_row())
        assert embed["url"] == "https://funda.nl/abc123"

    def test_color_is_green(self):
        embed = _build_embed(self._make_row())
        assert embed["color"] == 0x00B894
