create table if not exists {{ schema }}.price_history (
    global_id text,
    price bigint,
    human_price text,
    date text,
    timestamp text,
    source text,
    status text,
    ingested_at timestamptz default now(),
    unique (global_id, date, source, status)
);
