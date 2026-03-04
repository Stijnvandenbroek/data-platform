-- Mart: per-city price statistics for available listings.

with listings as (
    select * from {{ ref('funda_listings') }}
    where not is_sold
),

city_stats as (
    select
        city,
        province,
        offering_type,
        object_type,
        count(*) as listing_count,
        round(avg(current_price), 0) as avg_price,
        min(current_price) as min_price,
        max(current_price) as max_price,
        percentile_cont(0.5) within group (
            order by current_price
        ) as median_price,
        round(avg(price_per_sqm), 0) as avg_price_per_sqm,
        round(avg(living_area), 0) as avg_living_area,
        round(avg(bedrooms), 1) as avg_bedrooms
    from listings
    where current_price is not null
    group by city, province, offering_type, object_type
)

select * from city_stats
