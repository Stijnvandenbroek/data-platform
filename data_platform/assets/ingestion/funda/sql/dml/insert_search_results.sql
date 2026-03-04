INSERT INTO {{ schema }}.search_results (
    global_id, title, city, postcode, province, neighbourhood,
    price, living_area, plot_area, bedrooms, rooms, energy_label,
    object_type, offering_type, construction_type, publish_date,
    broker_id, broker_name, raw_json
)
VALUES (
    :global_id, :title, :city, :postcode, :province, :neighbourhood,
    :price, :living_area, :plot_area, :bedrooms, :rooms, :energy_label,
    :object_type, :offering_type, :construction_type, :publish_date,
    :broker_id, :broker_name, :raw_json
)
ON CONFLICT (global_id) DO UPDATE SET
    title = excluded.title,
    city = excluded.city,
    postcode = excluded.postcode,
    province = excluded.province,
    neighbourhood = excluded.neighbourhood,
    price = excluded.price,
    living_area = excluded.living_area,
    plot_area = excluded.plot_area,
    bedrooms = excluded.bedrooms,
    rooms = excluded.rooms,
    energy_label = excluded.energy_label,
    object_type = excluded.object_type,
    offering_type = excluded.offering_type,
    construction_type = excluded.construction_type,
    publish_date = excluded.publish_date,
    broker_id = excluded.broker_id,
    broker_name = excluded.broker_name,
    raw_json = excluded.raw_json,
    ingested_at = now(),
    last_seen_at = now(),
    is_active = TRUE
