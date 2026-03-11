select
    ep.global_id,
    ep.predicted_elo,
    fl.title,
    fl.city,
    fl.url,
    fl.current_price,
    fl.living_area,
    fl.bedrooms,
    fl.rooms,
    fl.energy_label,
    fl.price_per_sqm,
    ep.scored_at
from elo.predictions as ep
inner join marts.funda_listings as fl on ep.global_id = fl.global_id
left join elo.notified as en on ep.global_id = en.global_id
where
    ep.predicted_elo >= :min_elo
    and en.global_id is null
order by ep.predicted_elo desc
