"""LightGBM model to predict ELO ratings for Funda listings."""

from pathlib import Path

import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd
from sqlalchemy import text
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Config,
    MaterializeResult,
    MetadataValue,
    asset,
)
from lightgbm import LGBMRegressor
from sklearn.model_selection import train_test_split

from data_platform.helpers import render_sql
from data_platform.resources import MLflowResource, PostgresResource

_SQL_DIR = Path(__file__).parent / "sql"

# Energy label → ordinal int (higher = better)
ENERGY_LABEL_MAP: dict[str | None, int] = {
    "A5": 10,
    "A4": 9,
    "A3": 8,
    "A2": 7,
    "A1": 6,
    "A": 5,
    "B": 4,
    "C": 3,
    "D": 2,
    "E": 1,
    "F": 0,
    "G": -1,
}

_MAX_RETAINED_RUNS = 3

NUMERIC_FEATURES = [
    "current_price",
    "living_area",
    "plot_area",
    "bedrooms",
    "rooms",
    "construction_year",
    "latitude",
    "longitude",
    "photo_count",
    "views",
    "saves",
    "price_per_sqm",
]
BOOL_FEATURES = [
    "has_garden",
    "has_balcony",
    "has_solar_panels",
    "has_heat_pump",
    "has_roof_terrace",
    "is_energy_efficient",
    "is_monument",
]
DERIVED_FEATURES = [
    "energy_label_num",
]

ALL_FEATURES = NUMERIC_FEATURES + BOOL_FEATURES + DERIVED_FEATURES


class EloModelConfig(Config):
    """Training hyper-parameters and options."""

    test_size: float = 0.2
    random_state: int = 42
    min_comparisons: int = 5
    n_estimators: int = 200
    learning_rate: float = 0.05
    max_depth: int = 6
    num_leaves: int = 31
    mlflow_experiment: str = "elo-rating-prediction"


def _preprocess(df: pd.DataFrame) -> pd.DataFrame:
    """Convert raw columns to model-ready numeric features."""
    df["energy_label_num"] = (
        df["energy_label"]
        .str.strip()
        .str.upper()
        .map(ENERGY_LABEL_MAP)
        .fillna(-2)
        .astype(int)
    )

    for col in BOOL_FEATURES:
        df[col] = df[col].fillna(False).astype(int)

    for col in NUMERIC_FEATURES:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        median = df[col].median()
        df[col] = df[col].fillna(median if pd.notna(median) else 0)

    return df


@asset(
    deps=["elo_ratings", AssetKey(["marts", "funda_listings"])],
    group_name="ml",
    kinds={"python", "mlflow", "lightgbm"},
    tags={"manual": "true"},
    description=(
        "Train a LightGBM regressor to predict normalised ELO rating from "
        "listing features.  Logs the model, parameters and metrics to MLflow."
    ),
)
def elo_prediction_model(
    context: AssetExecutionContext,
    config: EloModelConfig,
    postgres: PostgresResource,
    mlflow_resource: MLflowResource,
) -> MaterializeResult:
    # Fetch training data
    engine = postgres.get_engine()
    query = render_sql(_SQL_DIR, "select_training_data.sql")
    df = pd.read_sql(
        text(query),
        engine,
        params={"min_comparisons": config.min_comparisons},
    )
    context.log.info(f"Loaded {len(df)} listings with ELO ratings.")

    if len(df) < 10:
        raise ValueError(
            f"Not enough rated listings ({len(df)}). "
            "Need at least 10 rows with sufficient comparisons."
        )

    # Preprocess and normalise ELO target
    df = _preprocess(df)
    df["elo_norm"] = (df["elo_rating"] - 1500) / 100

    X = df[ALL_FEATURES].copy()
    y = df["elo_norm"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=config.test_size, random_state=config.random_state
    )
    context.log.info(f"Train set: {len(X_train)} rows, test set: {len(X_test)} rows.")

    # Train model
    mlflow.set_tracking_uri(mlflow_resource.get_tracking_uri())
    mlflow.set_experiment(config.mlflow_experiment)

    with mlflow.start_run() as run:
        model = LGBMRegressor(
            n_estimators=config.n_estimators,
            learning_rate=config.learning_rate,
            max_depth=config.max_depth,
            num_leaves=config.num_leaves,
            random_state=config.random_state,
            verbosity=-1,
        )
        model.fit(
            X_train,
            y_train,
            eval_set=[(X_test, y_test)],
            eval_metric="rmse",
        )

        # Evaluate
        y_pred = model.predict(X_test)
        rmse = float(np.sqrt(np.mean((y_test - y_pred) ** 2)))
        mae = float(np.mean(np.abs(y_test - y_pred)))
        r2 = float(
            1 - np.sum((y_test - y_pred) ** 2) / np.sum((y_test - y_test.mean()) ** 2)
        )

        context.log.info(f"RMSE: {rmse:.4f}  MAE: {mae:.4f}  R²: {r2:.4f}")

        # Log params, metrics and model to MLflow
        mlflow.log_params(
            {
                "n_estimators": config.n_estimators,
                "learning_rate": config.learning_rate,
                "max_depth": config.max_depth,
                "num_leaves": config.num_leaves,
                "test_size": config.test_size,
                "min_comparisons": config.min_comparisons,
                "train_rows": len(X_train),
                "test_rows": len(X_test),
                "features": ", ".join(ALL_FEATURES),
            }
        )
        mlflow.log_metrics({"rmse": rmse, "mae": mae, "r2": r2})

        importances = dict(
            zip(ALL_FEATURES, model.feature_importances_.tolist(), strict=False)
        )
        for feat, imp in importances.items():
            mlflow.log_metric(f"importance_{feat}", imp)

        mlflow.lightgbm.log_model(
            model,
            artifact_path="elo_lgbm_model",
            input_example=X_test.iloc[:1],
        )

        run_id = run.info.run_id

    context.log.info(
        f"MLflow run {run_id} logged to experiment '{config.mlflow_experiment}'."
    )

    # Delete old runs beyond retention limit
    _cleanup_old_runs(config.mlflow_experiment, context)

    # Build feature importance table for Dagster metadata
    imp_sorted = sorted(importances.items(), key=lambda x: x[1], reverse=True)
    imp_md = "| Feature | Importance |\n|---|---|\n"
    imp_md += "\n".join(f"| {f} | {v} |" for f, v in imp_sorted)

    return MaterializeResult(
        metadata={
            "mlflow_run_id": MetadataValue.text(run_id),
            "mlflow_experiment": MetadataValue.text(config.mlflow_experiment),
            "train_rows": len(X_train),
            "test_rows": len(X_test),
            "rmse": MetadataValue.float(rmse),
            "mae": MetadataValue.float(mae),
            "r2": MetadataValue.float(r2),
            "feature_importances": MetadataValue.md(imp_md),
        }
    )


def _cleanup_old_runs(
    experiment_name: str,
    context: AssetExecutionContext,
    keep: int = _MAX_RETAINED_RUNS,
) -> None:
    """Delete oldest MLflow runs, keeping only the most recent *keep*."""
    client = mlflow.tracking.MlflowClient()
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment is None:
        return

    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["start_time DESC"],
    )

    if len(runs) <= keep:
        return

    stale_runs = runs[keep:]
    for run in stale_runs:
        context.log.info(f"Deleting old MLflow run {run.info.run_id}")
        client.delete_run(run.info.run_id)

    context.log.info(
        f"Retained {keep} runs, deleted {len(stale_runs)} old run(s) "
        f"from experiment '{experiment_name}'."
    )
