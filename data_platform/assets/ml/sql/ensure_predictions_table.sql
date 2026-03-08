create table if not exists elo.predictions (
    global_id text primary key,
    predicted_elo double precision not null,
    mlflow_run_id text not null,
    scored_at timestamp with time zone default now()
)
