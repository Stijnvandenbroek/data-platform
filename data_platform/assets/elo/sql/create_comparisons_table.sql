create table if not exists {{ schema }}.comparisons (
    id serial primary key,
    listing_a_id text not null,
    listing_b_id text not null,
    winner_id text not null,
    elo_a_before double precision not null,
    elo_b_before double precision not null,
    elo_a_after double precision not null,
    elo_b_after double precision not null,
    created_at timestamptz default now()
);
