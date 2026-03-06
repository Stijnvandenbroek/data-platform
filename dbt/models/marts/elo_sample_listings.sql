-- Mart: stable random sample of Funda listings for pairwise ELO comparison.
-- Incrementally tops up to the target sample size using deterministic ordering.

select l.global_id
from {{ ref('funda_listings') }} as l
{% if is_incremental() %}
    left join {{ this }} as s on l.global_id = s.global_id
    where s.global_id is null
    order by md5(l.global_id)
    limit greatest(0, 50 - (select count(*) from {{ this }}))
{% else %}
    order by md5(l.global_id)
    limit 50
{% endif %}
