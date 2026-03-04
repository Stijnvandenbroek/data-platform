CREATE TABLE IF NOT EXISTS {{ schema }}.price_history (
    global_id TEXT,
    price BIGINT,
    human_price TEXT,
    date TEXT,
    timestamp TEXT,
    source TEXT,
    status TEXT,
    ingested_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (global_id, date, source, status)
);
