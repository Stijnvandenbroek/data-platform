create table if not exists {{ schema }}.ratings (
    global_id text primary key,
    elo_rating double precision not null default 1500.0,
    comparison_count integer not null default 0,
    wins integer not null default 0,
    losses integer not null default 0,
    created_at timestamptz default now(),
    updated_at timestamptz default now()
);
