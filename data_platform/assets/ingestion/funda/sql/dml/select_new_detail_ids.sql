select distinct s.global_id
from {{ schema }}.search_results as s
left join {{ schema }}.listing_details as d on s.global_id = d.global_id
where
    s.is_active = true
    and (d.global_id is null or d.is_stale = true)
