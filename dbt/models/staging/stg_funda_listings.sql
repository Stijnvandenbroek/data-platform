-- Staging model: one row per Funda listing with cleaned core fields.

with source as (
    select * from {{ source('raw_funda', 'listing_details') }}
),

staged as (
    select
        global_id,
        tiny_id,
        title,
        city,
        postcode,
        province,
        neighbourhood,
        municipality,
        price,
        price_formatted,
        status,
        offering_type,
        object_type,
        house_type,
        construction_type,
        construction_year,
        energy_label,
        living_area,
        plot_area,
        bedrooms,
        rooms,
        publication_date,
        latitude,
        longitude,
        has_garden,
        has_balcony,
        has_solar_panels,
        has_heat_pump,
        has_roof_terrace,
        is_energy_efficient,
        is_monument,
        url,
        photo_count,
        views,
        saves,
        ingested_at
    from source
)

select * from staged
