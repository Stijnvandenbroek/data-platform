update {{ schema }}.listing_details d
set is_stale = true
from {{ schema }}.search_results as s
where
    d.global_id = s.global_id
    and s.is_active = false
    and d.is_stale = false
