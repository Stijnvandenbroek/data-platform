insert into elo.predictions (global_id, predicted_elo, mlflow_run_id)
values (:global_id, :predicted_elo, :mlflow_run_id)
on conflict (global_id) do update
    set
        predicted_elo = excluded.predicted_elo,
        mlflow_run_id = excluded.mlflow_run_id,
        scored_at = now()
