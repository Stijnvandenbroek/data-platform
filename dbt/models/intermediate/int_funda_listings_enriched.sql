-- Intermediate model: enrich each listing with its most recent asking price
-- and the last recorded sold price from the price history.

with listings as (
    select * from {{ ref('stg_funda_listings') }}
),

price_history as (
    select * from {{ ref('stg_funda_price_history') }}
),

latest_asking as (
    select distinct on (global_id)
        global_id,
        price as latest_asking_price,
        price_date as latest_asking_date
    from price_history
    where
        price_source = 'Funda'
        and price_status = 'asking_price'
    order by global_id asc, price_date desc
),

latest_sold as (
    select distinct on (global_id)
        global_id,
        price as sold_price,
        price_date as sold_date
    from price_history
    where price_status = 'sold'
    order by global_id asc, price_date desc
),

enriched as (
    select
        l.*,
        la.latest_asking_price,
        la.latest_asking_date,
        ls.sold_price,
        ls.sold_date,
        ls.sold_price is not null as is_sold
    from listings as l
    left join latest_asking as la on l.global_id = la.global_id
    left join latest_sold as ls on l.global_id = ls.global_id
)

select * from enriched
