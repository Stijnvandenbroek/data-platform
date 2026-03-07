with last_price_history as (
    select
        global_id,
        max(ingested_at) as last_ingested
    from {{ schema }}.price_history
    group by global_id
)

select distinct
    d.global_id,
    d.url,
    d.title,
    d.postcode
from {{ schema }}.listing_details as d
inner join {{ schema }}.search_results as s
    on d.global_id = s.global_id
left join last_price_history as ph
    on d.global_id = ph.global_id
where
    s.is_active = true
    and (
        ph.last_ingested is null
        or ph.last_ingested < now() - interval '{{ staleness_days }} days'
    )
