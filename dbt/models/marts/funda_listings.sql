-- Mart: analysis-ready Funda listings table.
-- Selects the most useful fields and adds derived metrics.

with enriched as (
    select * from {{ ref('int_funda_listings_enriched') }}
),

final as (
    select
        -- identifiers
        global_id,
        tiny_id,
        url,

        -- location
        title,
        city,
        postcode,
        province,
        neighbourhood,
        municipality,
        latitude,
        longitude,

        -- property characteristics
        object_type,
        house_type,
        offering_type,
        construction_type,
        construction_year,
        energy_label,
        living_area,
        plot_area,
        bedrooms,
        rooms,
        has_garden,
        has_balcony,
        has_solar_panels,
        has_heat_pump,
        has_roof_terrace,
        is_energy_efficient,
        is_monument,

        -- pricing
        price as current_price,
        latest_asking_price,
        latest_asking_date,
        sold_price,
        sold_date,
        is_sold,

        -- derived
        photo_count,

        -- engagement
        views,
        saves,
        status,

        -- meta
        publication_date,
        ingested_at,
        case
            when living_area > 0 then round(price::numeric / living_area, 0)
        end as price_per_sqm
    from enriched
)

select * from final
