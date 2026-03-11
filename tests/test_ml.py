"""Tests for ML helper functions and asset functions."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from dagster import build_asset_context

from data_platform.assets.ml.elo_model import (
    ALL_FEATURES,
    ENERGY_LABEL_MAP,
    _cleanup_old_runs,
    preprocess,
)
from data_platform.assets.ml.discord_alerts import (
    DiscordNotificationConfig,
    _build_embed,
)
from data_platform.assets.ml.elo_inference import EloInferenceConfig, _best_run


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


# ---------------------------------------------------------------------------
# _cleanup_old_runs
# ---------------------------------------------------------------------------


class TestCleanupOldRuns:
    @patch("data_platform.assets.ml.elo_model.mlflow")
    def test_no_experiment_noop(self, mock_mlflow):
        client = mock_mlflow.tracking.MlflowClient.return_value
        client.get_experiment_by_name.return_value = None
        context = MagicMock()
        _cleanup_old_runs("nonexistent", context, keep=3)
        client.delete_run.assert_not_called()

    @patch("data_platform.assets.ml.elo_model.mlflow")
    def test_fewer_than_keep_noop(self, mock_mlflow):
        client = mock_mlflow.tracking.MlflowClient.return_value
        client.get_experiment_by_name.return_value = MagicMock(experiment_id="1")
        client.search_runs.return_value = [MagicMock(), MagicMock()]
        context = MagicMock()
        _cleanup_old_runs("exp", context, keep=3)
        client.delete_run.assert_not_called()

    @patch("data_platform.assets.ml.elo_model.mlflow")
    def test_deletes_old_runs(self, mock_mlflow):
        client = mock_mlflow.tracking.MlflowClient.return_value
        client.get_experiment_by_name.return_value = MagicMock(experiment_id="1")
        runs = [MagicMock() for _ in range(5)]
        for i, run in enumerate(runs):
            run.info.run_id = f"run_{i}"
        client.search_runs.return_value = runs
        context = MagicMock()
        _cleanup_old_runs("exp", context, keep=2)
        assert client.delete_run.call_count == 3
        client.delete_run.assert_any_call("run_2")
        client.delete_run.assert_any_call("run_3")
        client.delete_run.assert_any_call("run_4")

    @patch("data_platform.assets.ml.elo_model.mlflow")
    def test_exactly_keep_noop(self, mock_mlflow):
        client = mock_mlflow.tracking.MlflowClient.return_value
        client.get_experiment_by_name.return_value = MagicMock(experiment_id="1")
        client.search_runs.return_value = [MagicMock(), MagicMock(), MagicMock()]
        context = MagicMock()
        _cleanup_old_runs("exp", context, keep=3)
        client.delete_run.assert_not_called()


# ---------------------------------------------------------------------------
# elo_prediction_model asset
# ---------------------------------------------------------------------------


def _make_training_df(n=20):
    """Build a small training DataFrame with all required columns."""
    rng = np.random.default_rng(42)
    data = {
        "energy_label": rng.choice(["A", "B", "C", "D"], size=n),
        "current_price": rng.integers(200_000, 500_000, size=n).astype(float),
        "living_area": rng.integers(40, 150, size=n).astype(float),
        "plot_area": rng.integers(0, 300, size=n).astype(float),
        "bedrooms": rng.integers(1, 5, size=n).astype(float),
        "rooms": rng.integers(2, 8, size=n).astype(float),
        "construction_year": rng.integers(1900, 2025, size=n).astype(float),
        "latitude": rng.uniform(51, 53, size=n),
        "longitude": rng.uniform(3, 6, size=n),
        "photo_count": rng.integers(1, 30, size=n).astype(float),
        "views": rng.integers(10, 500, size=n).astype(float),
        "saves": rng.integers(0, 50, size=n).astype(float),
        "price_per_sqm": rng.integers(2000, 6000, size=n).astype(float),
        "has_garden": rng.choice([True, False, None], size=n),
        "has_balcony": rng.choice([True, False], size=n),
        "has_solar_panels": rng.choice([True, False, None], size=n),
        "has_heat_pump": rng.choice([True, False], size=n),
        "has_roof_terrace": rng.choice([True, False, None], size=n),
        "is_energy_efficient": rng.choice([True, False], size=n),
        "is_monument": rng.choice([True, False], size=n),
        "elo_rating": rng.uniform(1200, 1800, size=n),
    }
    return pd.DataFrame(data)


class TestEloPredictionModelAsset:
    @patch("data_platform.assets.ml.elo_model._cleanup_old_runs")
    @patch("data_platform.assets.ml.elo_model.mlflow")
    def test_trains_and_returns_metadata(self, mock_mlflow, mock_cleanup):
        # Setup MLflow mock
        mock_run = MagicMock()
        mock_run.info.run_id = "test-run-123"
        mock_mlflow.start_run.return_value.__enter__ = MagicMock(return_value=mock_run)
        mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

        # Setup Postgres mock
        postgres = MagicMock()
        engine = MagicMock()
        postgres.get_engine.return_value = engine
        mlflow_resource = MagicMock()
        mlflow_resource.get_tracking_uri.return_value = "http://mlflow:5000"

        df = _make_training_df(20)
        context = build_asset_context()

        from data_platform.assets.ml.elo_model import (
            EloModelConfig,
            elo_prediction_model,
        )

        config = EloModelConfig(n_estimators=10)

        with patch("data_platform.assets.ml.elo_model.pd.read_sql", return_value=df):
            result = elo_prediction_model(context, config, postgres, mlflow_resource)

        assert result.metadata["mlflow_run_id"].value == "test-run-123"
        assert "rmse" in result.metadata
        assert "mae" in result.metadata
        assert "r2" in result.metadata
        assert result.metadata["train_rows"] > 0
        assert result.metadata["test_rows"] > 0
        mock_cleanup.assert_called_once()

    @patch("data_platform.assets.ml.elo_model.mlflow")
    def test_raises_with_too_few_rows(self, mock_mlflow):
        postgres = MagicMock()
        engine = MagicMock()
        postgres.get_engine.return_value = engine
        mlflow_resource = MagicMock()
        mlflow_resource.get_tracking_uri.return_value = "http://mlflow:5000"

        df = _make_training_df(5)  # fewer than 10
        context = build_asset_context()

        from data_platform.assets.ml.elo_model import (
            EloModelConfig,
            elo_prediction_model,
        )

        config = EloModelConfig()

        with (
            patch("data_platform.assets.ml.elo_model.pd.read_sql", return_value=df),
            pytest.raises(ValueError, match="Not enough rated listings"),
        ):
            elo_prediction_model(context, config, postgres, mlflow_resource)


# ---------------------------------------------------------------------------
# elo_inference asset
# ---------------------------------------------------------------------------


class TestEloInferenceAsset:
    def _make_unscored_df(self, n=5):
        """Build a DataFrame of unscored listings."""
        rng = np.random.default_rng(42)
        data = {
            "global_id": [f"listing_{i}" for i in range(n)],
            "energy_label": rng.choice(["A", "B", "C"], size=n),
            "current_price": rng.integers(200_000, 500_000, size=n).astype(float),
            "living_area": rng.integers(40, 150, size=n).astype(float),
            "plot_area": rng.integers(0, 300, size=n).astype(float),
            "bedrooms": rng.integers(1, 5, size=n).astype(float),
            "rooms": rng.integers(2, 8, size=n).astype(float),
            "construction_year": rng.integers(1900, 2025, size=n).astype(float),
            "latitude": rng.uniform(51, 53, size=n),
            "longitude": rng.uniform(3, 6, size=n),
            "photo_count": rng.integers(1, 30, size=n).astype(float),
            "views": rng.integers(10, 500, size=n).astype(float),
            "saves": rng.integers(0, 50, size=n).astype(float),
            "price_per_sqm": rng.integers(2000, 6000, size=n).astype(float),
            "has_garden": rng.choice([True, False], size=n),
            "has_balcony": rng.choice([True, False], size=n),
            "has_solar_panels": rng.choice([True, False], size=n),
            "has_heat_pump": rng.choice([True, False], size=n),
            "has_roof_terrace": rng.choice([True, False], size=n),
            "is_energy_efficient": rng.choice([True, False], size=n),
            "is_monument": rng.choice([True, False], size=n),
        }
        return pd.DataFrame(data)

    @patch("data_platform.assets.ml.elo_inference.mlflow")
    @patch("data_platform.assets.ml.elo_inference.pd.read_sql")
    @patch("data_platform.assets.ml.elo_inference.render_sql", return_value="SQL")
    def test_scores_listings(self, mock_render, mock_read_sql, mock_mlflow):
        from data_platform.assets.ml.elo_inference import elo_inference

        df = self._make_unscored_df(5)
        mock_read_sql.return_value = df

        # Mock MLflow best run
        best_run = MagicMock()
        best_run.info.run_id = "run-abc"
        best_run.data.metrics = {"rmse": 0.5}
        client = mock_mlflow.tracking.MlflowClient.return_value
        client.get_experiment_by_name.return_value = MagicMock(experiment_id="1")
        client.search_runs.return_value = [best_run]

        # Mock model
        mock_model = MagicMock()
        mock_model.predict.return_value = np.random.default_rng(42).uniform(
            -3, 3, size=5
        )
        mock_mlflow.lightgbm.load_model.return_value = mock_model

        # Mock resources
        postgres = MagicMock()
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        postgres.get_engine.return_value = engine

        mlflow_resource = MagicMock()
        mlflow_resource.get_tracking_uri.return_value = "http://mlflow:5000"

        context = build_asset_context()
        config = EloInferenceConfig()

        result = elo_inference(context, config, postgres, mlflow_resource)

        assert result.metadata["scored"] == 5
        assert result.metadata["mlflow_run_id"].value == "run-abc"
        assert "predicted_elo_mean" in result.metadata
        mock_model.predict.assert_called_once()

    @patch("data_platform.assets.ml.elo_inference.pd.read_sql")
    @patch("data_platform.assets.ml.elo_inference.render_sql", return_value="SQL")
    def test_empty_returns_zero_scored(self, mock_render, mock_read_sql):
        from data_platform.assets.ml.elo_inference import elo_inference

        mock_read_sql.return_value = pd.DataFrame()

        postgres = MagicMock()
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        postgres.get_engine.return_value = engine

        mlflow_resource = MagicMock()
        context = build_asset_context()
        config = EloInferenceConfig()

        result = elo_inference(context, config, postgres, mlflow_resource)

        assert result.metadata["scored"] == 0
        assert result.metadata["status"].value == "No new listings to score."


# ---------------------------------------------------------------------------
# listing_alert asset
# ---------------------------------------------------------------------------


class TestListingAlertAsset:
    def _make_predictions_df(self, n=3):
        """Build a DataFrame of high-ELO predictions."""
        data = {
            "global_id": [f"listing_{i}" for i in range(n)],
            "predicted_elo": [1650.0 + i * 10 for i in range(n)],
            "current_price": [350_000 + i * 10_000 for i in range(n)],
            "city": ["Amsterdam", "Rotterdam", "Utrecht"][:n],
            "living_area": [80 + i * 5 for i in range(n)],
            "rooms": [4, 5, 3][:n],
            "energy_label": ["A", "B", "C"][:n],
            "price_per_sqm": [4375, 4000, 3800][:n],
            "title": [f"Teststraat {i}" for i in range(n)],
            "url": [f"https://funda.nl/{i}" for i in range(n)],
        }
        return pd.DataFrame(data)

    @patch("data_platform.assets.ml.discord_alerts.requests.post")
    @patch("data_platform.assets.ml.discord_alerts.pd.read_sql")
    @patch("data_platform.assets.ml.discord_alerts.render_sql", return_value="SQL")
    def test_sends_notifications(self, mock_render, mock_read_sql, mock_post):
        from data_platform.assets.ml.discord_alerts import listing_alert

        df = self._make_predictions_df(3)
        mock_read_sql.return_value = df
        mock_post.return_value = MagicMock(status_code=200)

        postgres = MagicMock()
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        postgres.get_engine.return_value = engine

        discord = MagicMock()
        discord.get_webhook_url.return_value = "https://discord.com/webhook/test"

        context = build_asset_context()
        config = DiscordNotificationConfig()

        result = listing_alert(context, config, postgres, discord)

        assert result.metadata["notified"] == 3
        mock_post.assert_called_once()

    @patch("data_platform.assets.ml.discord_alerts.pd.read_sql")
    @patch("data_platform.assets.ml.discord_alerts.render_sql", return_value="SQL")
    def test_empty_returns_zero_notified(self, mock_render, mock_read_sql):
        from data_platform.assets.ml.discord_alerts import listing_alert

        mock_read_sql.return_value = pd.DataFrame()

        postgres = MagicMock()
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        postgres.get_engine.return_value = engine

        discord = MagicMock()
        context = build_asset_context()
        config = DiscordNotificationConfig()

        result = listing_alert(context, config, postgres, discord)

        assert result.metadata["notified"] == 0

    @patch("data_platform.assets.ml.discord_alerts.requests.post")
    @patch("data_platform.assets.ml.discord_alerts.pd.read_sql")
    @patch("data_platform.assets.ml.discord_alerts.render_sql", return_value="SQL")
    def test_batches_large_result_set(self, mock_render, mock_read_sql, mock_post):
        from data_platform.assets.ml.discord_alerts import listing_alert

        # 15 listings → should produce 2 batches (10 + 5)
        data = {
            "global_id": [f"listing_{i}" for i in range(15)],
            "predicted_elo": [1700.0] * 15,
            "current_price": [400_000] * 15,
            "city": ["Amsterdam"] * 15,
            "living_area": [90] * 15,
            "rooms": [4] * 15,
            "energy_label": ["A"] * 15,
            "price_per_sqm": [4444] * 15,
            "title": [f"Street {i}" for i in range(15)],
            "url": [f"https://funda.nl/{i}" for i in range(15)],
        }
        mock_read_sql.return_value = pd.DataFrame(data)
        mock_post.return_value = MagicMock(status_code=200)

        postgres = MagicMock()
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)
        postgres.get_engine.return_value = engine

        discord = MagicMock()
        discord.get_webhook_url.return_value = "https://discord.com/webhook/test"

        context = build_asset_context()
        config = DiscordNotificationConfig()

        result = listing_alert(context, config, postgres, discord)

        assert result.metadata["notified"] == 15
        assert mock_post.call_count == 2


# ---------------------------------------------------------------------------
# Config defaults
# ---------------------------------------------------------------------------


class TestMLConfigs:
    def test_elo_inference_config_defaults(self):
        config = EloInferenceConfig()
        assert config.mlflow_experiment == "elo-rating-prediction"
        assert config.metric == "rmse"
        assert config.ascending is True

    def test_discord_notification_config_defaults(self):
        config = DiscordNotificationConfig()
        assert config.min_elo == 1600
