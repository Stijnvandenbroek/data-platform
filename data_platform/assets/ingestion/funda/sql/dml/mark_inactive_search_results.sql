update {{ schema }}.search_results
set is_active = false
where last_seen_at < now() - interval '7 days'
returning global_id
