-- Staging model: one row per price-history event per listing.

with source as (
    select * from {{ source('raw_funda', 'price_history') }}
),

staged as (
    select
        global_id,
        price,
        human_price,
        date            as price_date,
        timestamp       as price_timestamp,
        source          as price_source,
        status          as price_status,
        ingested_at
    from source
)

select * from staged
