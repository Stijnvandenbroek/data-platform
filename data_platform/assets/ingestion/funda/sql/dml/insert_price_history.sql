-- noqa: disable=LT02
insert into {{ schema }}.price_history (
    global_id, price, human_price, date, timestamp, source, status
)
values (
    :global_id, :price, :human_price, :date, :timestamp, :source, :status
)
on conflict (global_id, date, source, status) do update set
    price = excluded.price,
    human_price = excluded.human_price,
    timestamp = excluded.timestamp,
    ingested_at = now()
