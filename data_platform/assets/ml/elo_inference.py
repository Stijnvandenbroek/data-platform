"""Infer ELO scores for new listings using the best trained model."""

from pathlib import Path

import mlflow
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Config,
    MaterializeResult,
    MetadataValue,
    asset,
)
from sqlalchemy import text

from data_platform.assets.ml.elo_model import (
    ALL_FEATURES,
    preprocess,
)
from data_platform.helpers import render_sql
from data_platform.resources import MLflowResource, PostgresResource

_SQL_DIR = Path(__file__).parent / "sql"


class EloInferenceConfig(Config):
    """Configuration for ELO inference."""

    mlflow_experiment: str = "elo-rating-prediction"
    metric: str = "rmse"
    ascending: bool = True


def _best_run(experiment_name: str, metric: str, ascending: bool):
    """Return the MLflow run with the best metric value."""
    client = mlflow.tracking.MlflowClient()
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment is None:
        raise ValueError(
            f"MLflow experiment '{experiment_name}' does not exist. "
            "Train the elo_prediction_model asset first."
        )

    order = "ASC" if ascending else "DESC"
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=[f"metrics.{metric} {order}"],
        max_results=1,
    )
    if not runs:
        raise ValueError(
            f"No runs found in experiment '{experiment_name}'. "
            "Train the elo_prediction_model asset first."
        )
    return runs[0]


@asset(
    deps=["elo_prediction_model", AssetKey(["marts", "funda_listings"])],
    group_name="ml",
    kinds={"python", "mlflow"},
    tags={"manual": "true"},
    description=(
        "Load the best ELO prediction model from MLflow and infer scores "
        "for all listings that have not been scored yet."
    ),
)
def elo_inference(
    context: AssetExecutionContext,
    config: EloInferenceConfig,
    postgres: PostgresResource,
    mlflow_resource: MLflowResource,
) -> MaterializeResult:
    engine = postgres.get_engine()

    # Ensure target table exists
    with engine.begin() as conn:
        conn.execute(text(render_sql(_SQL_DIR, "ensure_elo_schema.sql")))
        conn.execute(text(render_sql(_SQL_DIR, "ensure_predictions_table.sql")))

    # Fetch unscored listings
    query = render_sql(_SQL_DIR, "select_unscored_listings.sql")
    df = pd.read_sql(text(query), engine)
    context.log.info(f"Found {len(df)} unscored listings.")

    if df.empty:
        return MaterializeResult(
            metadata={
                "scored": 0,
                "status": MetadataValue.text("No new listings to score."),
            }
        )

    # Load best model
    mlflow.set_tracking_uri(mlflow_resource.get_tracking_uri())
    best_run = _best_run(config.mlflow_experiment, config.metric, config.ascending)
    run_id = best_run.info.run_id
    model_uri = f"runs:/{run_id}/elo_lgbm_model"
    context.log.info(
        f"Loading model from run {run_id} "
        f"({config.metric}={best_run.data.metrics.get(config.metric, '?')})."
    )
    model = mlflow.lightgbm.load_model(model_uri)

    # Preprocess features identically to training
    df = preprocess(df)
    X = df[ALL_FEATURES].copy()

    # Predict normalised ELO and convert back to original scale
    elo_norm = model.predict(X)
    df["predicted_elo"] = elo_norm * 100 + 1500

    # Write predictions
    rows = [
        {
            "global_id": row.global_id,
            "predicted_elo": float(row.predicted_elo),
            "mlflow_run_id": run_id,
        }
        for row in df.itertuples()
    ]
    upsert = render_sql(_SQL_DIR, "upsert_prediction.sql")
    with engine.begin() as conn:
        conn.execute(text(upsert), rows)

    context.log.info(f"Wrote {len(rows)} predictions (run {run_id}).")

    return MaterializeResult(
        metadata={
            "scored": len(rows),
            "mlflow_run_id": MetadataValue.text(run_id),
            "predicted_elo_mean": MetadataValue.float(
                float(df["predicted_elo"].mean())
            ),
            "predicted_elo_std": MetadataValue.float(float(df["predicted_elo"].std())),
        }
    )
