select distinct
    d.global_id,
    d.url,
    d.title,
    d.postcode
from {{ schema }}.listing_details as d
inner join {{ schema }}.search_results as s on d.global_id = s.global_id
where s.is_active = true
union
select distinct
    d.global_id,
    d.url,
    d.title,
    d.postcode
from {{ schema }}.listing_details as d
left join {{ schema }}.price_history as p on d.global_id = p.global_id
where p.global_id is null
