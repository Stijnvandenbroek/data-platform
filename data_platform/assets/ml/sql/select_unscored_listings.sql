select
    fl.global_id,
    fl.url,
    fl.title,
    fl.city,
    fl.current_price,
    fl.living_area,
    fl.plot_area,
    fl.bedrooms,
    fl.rooms,
    fl.construction_year,
    fl.latitude,
    fl.longitude,
    fl.energy_label,
    fl.has_garden,
    fl.has_balcony,
    fl.has_solar_panels,
    fl.has_heat_pump,
    fl.has_roof_terrace,
    fl.is_energy_efficient,
    fl.is_monument,
    fl.photo_count,
    fl.views,
    fl.saves,
    fl.price_per_sqm
from marts.funda_listings as fl
left join elo.predictions as ep on fl.global_id = ep.global_id
where ep.global_id is null
