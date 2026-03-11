select
    fl.global_id,
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
    fl.price_per_sqm,
    er.elo_rating
from marts.funda_listings as fl
inner join elo.ratings as er on fl.global_id = er.global_id
where er.comparison_count >= :min_comparisons
