create table if not exists elo.notified (
    global_id text primary key,
    notified_at timestamp with time zone default now()
)
