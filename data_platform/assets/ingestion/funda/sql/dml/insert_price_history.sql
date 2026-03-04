INSERT INTO {{ schema }}.price_history (
    global_id, price, human_price, date, timestamp, source, status
)
VALUES (
    :global_id, :price, :human_price, :date, :timestamp, :source, :status
)
ON CONFLICT (global_id, date, source, status) DO UPDATE SET
    price = excluded.price,
    human_price = excluded.human_price,
    timestamp = excluded.timestamp,
    ingested_at = now()
