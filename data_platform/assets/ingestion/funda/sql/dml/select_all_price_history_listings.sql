select distinct
    d.global_id,
    d.url,
    d.title,
    d.postcode
from {{ schema }}.listing_details as d
