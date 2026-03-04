"""Funda real-estate ingestion assets."""

import json

from dagster import (
    AssetExecutionContext,
    Config,
    MaterializeResult,
    MetadataValue,
    asset,
)
from sqlalchemy import text

from data_platform.helpers import (
    format_area,
    format_euro,
    md_preview_table,
    safe_int,
)
from data_platform.resources import FundaResource, PostgresResource


class FundaSearchConfig(Config):
    """Search parameters for Funda."""

    location: str = "woerden, utrecht, zeist, maarssen, nieuwegein, gouda"
    offering_type: str = "buy"
    price_min: int | None = 300000
    price_max: int | None = 500000
    area_min: int | None = None
    area_max: int | None = None
    plot_min: int | None = None
    plot_max: int | None = None
    object_type: str | None = None
    energy_label: str | None = None
    radius_km: int | None = None
    sort: str = "newest"
    max_pages: int = 3


class FundaDetailsConfig(Config):
    """Config for listing details fetch."""

    fetch_all: bool = True


class FundaPriceHistoryConfig(Config):
    """Config for price history fetch."""

    fetch_all: bool = True


_SCHEMA = "raw_funda"

_DDL_SEARCH = f"""
CREATE TABLE IF NOT EXISTS {_SCHEMA}.search_results (
    global_id       TEXT,
    title           TEXT,
    city            TEXT,
    postcode        TEXT,
    province        TEXT,
    neighbourhood   TEXT,
    price           BIGINT,
    living_area     INT,
    plot_area       INT,
    bedrooms        INT,
    rooms           INT,
    energy_label    TEXT,
    object_type     TEXT,
    offering_type   TEXT,
    construction_type TEXT,
    publish_date    TEXT,
    broker_id       TEXT,
    broker_name     TEXT,
    raw_json        JSONB,
    ingested_at     TIMESTAMPTZ DEFAULT now(),
    UNIQUE (global_id)
);
"""

_DDL_DETAILS = f"""
CREATE TABLE IF NOT EXISTS {_SCHEMA}.listing_details (
    global_id           TEXT,
    tiny_id             TEXT,
    title               TEXT,
    city                TEXT,
    postcode            TEXT,
    province            TEXT,
    neighbourhood       TEXT,
    municipality        TEXT,
    price               BIGINT,
    price_formatted     TEXT,
    status              TEXT,
    offering_type       TEXT,
    object_type         TEXT,
    house_type          TEXT,
    construction_type   TEXT,
    construction_year   TEXT,
    energy_label        TEXT,
    living_area         INT,
    plot_area           INT,
    bedrooms            INT,
    rooms               INT,
    description         TEXT,
    publication_date    TEXT,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    has_garden          BOOLEAN,
    has_balcony         BOOLEAN,
    has_solar_panels    BOOLEAN,
    has_heat_pump       BOOLEAN,
    has_roof_terrace    BOOLEAN,
    is_energy_efficient BOOLEAN,
    is_monument         BOOLEAN,
    url                 TEXT,
    photo_count         INT,
    views               INT,
    saves               INT,
    raw_json            JSONB,
    ingested_at         TIMESTAMPTZ DEFAULT now(),
    UNIQUE (global_id, status)
);
"""

_DDL_PRICE_HISTORY = f"""
CREATE TABLE IF NOT EXISTS {_SCHEMA}.price_history (
    global_id       TEXT,
    price           BIGINT,
    human_price     TEXT,
    date            TEXT,
    timestamp       TEXT,
    source          TEXT,
    status          TEXT,
    ingested_at     TIMESTAMPTZ DEFAULT now(),
    UNIQUE (global_id, date, source, status)
);
"""

# Deduplicate existing rows and add constraints for tables created before UNIQUE clauses.
_MIGRATE_SEARCH_CONSTRAINT = f"""
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = '{_SCHEMA}.search_results'::regclass
          AND contype = 'u'
    ) THEN
        DELETE FROM {_SCHEMA}.search_results a
        USING {_SCHEMA}.search_results b
        WHERE a.global_id = b.global_id
          AND a.ingested_at < b.ingested_at;

        ALTER TABLE {_SCHEMA}.search_results
            ADD UNIQUE (global_id);
    END IF;
END $$;
"""

_MIGRATE_DETAILS_CONSTRAINT = f"""
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = '{_SCHEMA}.listing_details'::regclass
          AND contype = 'u'
    ) THEN
        DELETE FROM {_SCHEMA}.listing_details a
        USING {_SCHEMA}.listing_details b
        WHERE a.global_id = b.global_id
          AND a.status IS NOT DISTINCT FROM b.status
          AND a.ingested_at < b.ingested_at;

        ALTER TABLE {_SCHEMA}.listing_details
            ADD UNIQUE (global_id, status);
    END IF;
END $$;
"""

_MIGRATE_PRICE_HISTORY_CONSTRAINT = f"""
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = '{_SCHEMA}.price_history'::regclass
          AND contype = 'u'
    ) THEN
        DELETE FROM {_SCHEMA}.price_history a
        USING {_SCHEMA}.price_history b
        WHERE a.global_id = b.global_id
          AND a.date IS NOT DISTINCT FROM b.date
          AND a.source IS NOT DISTINCT FROM b.source
          AND a.status IS NOT DISTINCT FROM b.status
          AND a.ingested_at < b.ingested_at;

        ALTER TABLE {_SCHEMA}.price_history
            ADD UNIQUE (global_id, date, source, status);
    END IF;
END $$;
"""


@asset(
    group_name="funda",
    kinds={"python", "postgres"},
    description="Search Funda listings and store results in Postgres.",
)
def funda_search_results(
    context: AssetExecutionContext,
    config: FundaSearchConfig,
    funda: FundaResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    client = funda.get_client()

    kwargs: dict = {
        "location": [loc.strip() for loc in config.location.split(",")],
        "offering_type": config.offering_type,
        "sort": config.sort,
    }
    if config.price_min is not None:
        kwargs["price_min"] = config.price_min
    if config.price_max is not None:
        kwargs["price_max"] = config.price_max
    if config.area_min is not None:
        kwargs["area_min"] = config.area_min
    if config.area_max is not None:
        kwargs["area_max"] = config.area_max
    if config.plot_min is not None:
        kwargs["plot_min"] = config.plot_min
    if config.plot_max is not None:
        kwargs["plot_max"] = config.plot_max
    if config.object_type:
        kwargs["object_type"] = [t.strip() for t in config.object_type.split(",")]
    if config.energy_label:
        kwargs["energy_label"] = [lbl.strip() for lbl in config.energy_label.split(",")]
    if config.radius_km is not None:
        kwargs["radius_km"] = config.radius_km

    all_listings = []
    for page in range(config.max_pages):
        context.log.info(f"Fetching search page {page + 1}/{config.max_pages} …")
        kwargs["page"] = page
        results = client.search_listing(**kwargs)
        if not results:
            context.log.info("No more results.")
            break
        all_listings.extend(results)
        context.log.info(f"  got {len(results)} listings (total: {len(all_listings)})")

    if not all_listings:
        context.log.warning("Search returned zero results.")
        return MaterializeResult(metadata={"count": 0})

    engine = postgres.get_engine()
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {_SCHEMA}"))
        conn.execute(text(_DDL_SEARCH))
        conn.execute(text(_MIGRATE_SEARCH_CONSTRAINT))

    rows = []
    for listing in all_listings:
        d = listing.to_dict()
        rows.append(
            {
                "global_id": d.get("global_id"),
                "title": d.get("title"),
                "city": d.get("city"),
                "postcode": d.get("postcode"),
                "province": d.get("province"),
                "neighbourhood": d.get("neighbourhood"),
                "price": safe_int(d.get("price")),
                "living_area": safe_int(d.get("living_area")),
                "plot_area": safe_int(d.get("plot_area")),
                "bedrooms": safe_int(d.get("bedrooms")),
                "rooms": safe_int(d.get("rooms")),
                "energy_label": d.get("energy_label"),
                "object_type": d.get("object_type"),
                "offering_type": d.get("offering_type"),
                "construction_type": d.get("construction_type"),
                "publish_date": d.get("publish_date"),
                "broker_id": str(d.get("broker_id", "")),
                "broker_name": d.get("broker_name"),
                "raw_json": json.dumps(d, default=str),
            }
        )

    insert_sql = f"""
        INSERT INTO {_SCHEMA}.search_results
            (global_id, title, city, postcode, province, neighbourhood,
             price, living_area, plot_area, bedrooms, rooms, energy_label,
             object_type, offering_type, construction_type, publish_date,
             broker_id, broker_name, raw_json)
        VALUES
            (:global_id, :title, :city, :postcode, :province, :neighbourhood,
             :price, :living_area, :plot_area, :bedrooms, :rooms, :energy_label,
             :object_type, :offering_type, :construction_type, :publish_date,
             :broker_id, :broker_name, :raw_json)
        ON CONFLICT (global_id) DO UPDATE SET
            title = EXCLUDED.title,
            city = EXCLUDED.city,
            postcode = EXCLUDED.postcode,
            province = EXCLUDED.province,
            neighbourhood = EXCLUDED.neighbourhood,
            price = EXCLUDED.price,
            living_area = EXCLUDED.living_area,
            plot_area = EXCLUDED.plot_area,
            bedrooms = EXCLUDED.bedrooms,
            rooms = EXCLUDED.rooms,
            energy_label = EXCLUDED.energy_label,
            object_type = EXCLUDED.object_type,
            offering_type = EXCLUDED.offering_type,
            construction_type = EXCLUDED.construction_type,
            publish_date = EXCLUDED.publish_date,
            broker_id = EXCLUDED.broker_id,
            broker_name = EXCLUDED.broker_name,
            raw_json = EXCLUDED.raw_json,
            ingested_at = now()
    """
    postgres.execute_many(insert_sql, rows)

    context.log.info(
        f"Inserted {len(rows)} search results into {_SCHEMA}.search_results"
    )

    return MaterializeResult(
        metadata={
            "count": len(rows),
            "location": MetadataValue.text(config.location),
            "offering_type": MetadataValue.text(config.offering_type),
            "preview": MetadataValue.md(
                md_preview_table(
                    rows[:10],
                    columns=[
                        ("title", "Title"),
                        ("city", "City"),
                        ("price", "Price"),
                        ("living_area", "Area"),
                        ("bedrooms", "Bedrooms"),
                    ],
                    formatters={"price": format_euro, "living_area": format_area},
                ),
            ),
        }
    )


@asset(
    group_name="funda",
    kinds={"python", "postgres"},
    deps=[funda_search_results],
    description="Fetch full listing details for each search result and store in Postgres.",
)
def funda_listing_details(
    context: AssetExecutionContext,
    config: FundaDetailsConfig,
    funda: FundaResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    client = funda.get_client()
    engine = postgres.get_engine()

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {_SCHEMA}"))
        conn.execute(text(_DDL_DETAILS))
        conn.execute(text(_MIGRATE_DETAILS_CONSTRAINT))

    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT DISTINCT global_id FROM {_SCHEMA}.search_results")
        )
        ids = [row[0] for row in result if row[0]]

    if not ids:
        context.log.warning("No search results found – run funda_search_results first.")
        return MaterializeResult(metadata={"count": 0})

    context.log.info(f"Fetching details for {len(ids)} listings …")

    rows = []
    errors = 0
    for i, gid in enumerate(ids):
        try:
            listing = client.get_listing(int(gid))
            d = listing.to_dict()
            rows.append(
                {
                    "global_id": d.get("global_id"),
                    "tiny_id": str(d.get("tiny_id", "")),
                    "title": d.get("title"),
                    "city": d.get("city"),
                    "postcode": d.get("postcode"),
                    "province": d.get("province"),
                    "neighbourhood": d.get("neighbourhood"),
                    "municipality": d.get("municipality"),
                    "price": safe_int(d.get("price")),
                    "price_formatted": d.get("price_formatted"),
                    "status": d.get("status"),
                    "offering_type": d.get("offering_type"),
                    "object_type": d.get("object_type"),
                    "house_type": d.get("house_type"),
                    "construction_type": d.get("construction_type"),
                    "construction_year": d.get("construction_year"),
                    "energy_label": d.get("energy_label"),
                    "living_area": safe_int(d.get("living_area")),
                    "plot_area": safe_int(d.get("plot_area")),
                    "bedrooms": safe_int(d.get("bedrooms")),
                    "rooms": safe_int(d.get("rooms")),
                    "description": d.get("description"),
                    "publication_date": d.get("publication_date"),
                    "latitude": d.get("latitude"),
                    "longitude": d.get("longitude"),
                    "has_garden": d.get("has_garden"),
                    "has_balcony": d.get("has_balcony"),
                    "has_solar_panels": d.get("has_solar_panels"),
                    "has_heat_pump": d.get("has_heat_pump"),
                    "has_roof_terrace": d.get("has_roof_terrace"),
                    "is_energy_efficient": d.get("is_energy_efficient"),
                    "is_monument": d.get("is_monument"),
                    "url": d.get("url"),
                    "photo_count": safe_int(d.get("photo_count")),
                    "views": safe_int(d.get("views")),
                    "saves": safe_int(d.get("saves")),
                    "raw_json": json.dumps(d, default=str),
                }
            )
        except Exception as e:
            errors += 1
            context.log.warning(f"Failed to fetch listing {gid}: {e}")
            continue

        if (i + 1) % 10 == 0:
            context.log.info(f"  fetched {i + 1}/{len(ids)} …")

    if rows:
        insert_sql = f"""
            INSERT INTO {_SCHEMA}.listing_details
                (global_id, tiny_id, title, city, postcode, province,
                 neighbourhood, municipality, price, price_formatted,
                 status, offering_type, object_type, house_type,
                 construction_type, construction_year, energy_label,
                 living_area, plot_area, bedrooms, rooms, description,
                 publication_date, latitude, longitude,
                 has_garden, has_balcony, has_solar_panels, has_heat_pump,
                 has_roof_terrace, is_energy_efficient, is_monument,
                 url, photo_count, views, saves, raw_json)
            VALUES
                (:global_id, :tiny_id, :title, :city, :postcode, :province,
                 :neighbourhood, :municipality, :price, :price_formatted,
                 :status, :offering_type, :object_type, :house_type,
                 :construction_type, :construction_year, :energy_label,
                 :living_area, :plot_area, :bedrooms, :rooms, :description,
                 :publication_date, :latitude, :longitude,
                 :has_garden, :has_balcony, :has_solar_panels, :has_heat_pump,
                 :has_roof_terrace, :is_energy_efficient, :is_monument,
                 :url, :photo_count, :views, :saves, :raw_json)
            ON CONFLICT (global_id, status) DO UPDATE SET
                tiny_id = EXCLUDED.tiny_id,
                title = EXCLUDED.title,
                city = EXCLUDED.city,
                postcode = EXCLUDED.postcode,
                province = EXCLUDED.province,
                neighbourhood = EXCLUDED.neighbourhood,
                municipality = EXCLUDED.municipality,
                price = EXCLUDED.price,
                price_formatted = EXCLUDED.price_formatted,
                offering_type = EXCLUDED.offering_type,
                object_type = EXCLUDED.object_type,
                house_type = EXCLUDED.house_type,
                construction_type = EXCLUDED.construction_type,
                construction_year = EXCLUDED.construction_year,
                energy_label = EXCLUDED.energy_label,
                living_area = EXCLUDED.living_area,
                plot_area = EXCLUDED.plot_area,
                bedrooms = EXCLUDED.bedrooms,
                rooms = EXCLUDED.rooms,
                description = EXCLUDED.description,
                publication_date = EXCLUDED.publication_date,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                has_garden = EXCLUDED.has_garden,
                has_balcony = EXCLUDED.has_balcony,
                has_solar_panels = EXCLUDED.has_solar_panels,
                has_heat_pump = EXCLUDED.has_heat_pump,
                has_roof_terrace = EXCLUDED.has_roof_terrace,
                is_energy_efficient = EXCLUDED.is_energy_efficient,
                is_monument = EXCLUDED.is_monument,
                url = EXCLUDED.url,
                photo_count = EXCLUDED.photo_count,
                views = EXCLUDED.views,
                saves = EXCLUDED.saves,
                raw_json = EXCLUDED.raw_json,
                ingested_at = now()
        """
        postgres.execute_many(insert_sql, rows)

    context.log.info(
        f"Inserted {len(rows)} listing details ({errors} errors) into {_SCHEMA}.listing_details"
    )

    return MaterializeResult(
        metadata={
            "count": len(rows),
            "errors": errors,
            "preview": MetadataValue.md(
                md_preview_table(
                    rows[:10],
                    columns=[
                        ("title", "Title"),
                        ("city", "City"),
                        ("price", "Price"),
                        ("status", "Status"),
                        ("energy_label", "Energy"),
                    ],
                    formatters={"price": format_euro},
                ),
            ),
        }
    )


@asset(
    group_name="funda",
    kinds={"python", "postgres"},
    deps=[funda_listing_details],
    description="Fetch price history for each detailed listing and store in Postgres.",
)
def funda_price_history(
    context: AssetExecutionContext,
    config: FundaPriceHistoryConfig,
    funda: FundaResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    client = funda.get_client()
    engine = postgres.get_engine()

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {_SCHEMA}"))
        conn.execute(text(_DDL_PRICE_HISTORY))
        conn.execute(text(_MIGRATE_PRICE_HISTORY_CONSTRAINT))

    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT DISTINCT global_id FROM {_SCHEMA}.listing_details")
        )
        ids = [row[0] for row in result if row[0]]

    if not ids:
        context.log.warning(
            "No listing details found – run funda_listing_details first."
        )
        return MaterializeResult(metadata={"count": 0})

    context.log.info(f"Fetching price history for {len(ids)} listings …")

    rows = []
    errors = 0
    for i, gid in enumerate(ids):
        try:
            listing = client.get_listing(int(gid))
            history = client.get_price_history(listing)
            for entry in history:
                rows.append(
                    {
                        "global_id": gid,
                        "price": safe_int(entry.get("price")),
                        "human_price": entry.get("human_price"),
                        "date": entry.get("date"),
                        "timestamp": entry.get("timestamp"),
                        "source": entry.get("source"),
                        "status": entry.get("status"),
                    }
                )
        except Exception as e:
            errors += 1
            context.log.warning(f"Failed to fetch price history for {gid}: {e}")
            continue

        if (i + 1) % 10 == 0:
            context.log.info(f"  fetched {i + 1}/{len(ids)} …")

    if rows:
        insert_sql = f"""
            INSERT INTO {_SCHEMA}.price_history
                (global_id, price, human_price, date, timestamp, source, status)
            VALUES
                (:global_id, :price, :human_price, :date, :timestamp, :source, :status)
            ON CONFLICT (global_id, date, source, status) DO UPDATE SET
                price = EXCLUDED.price,
                human_price = EXCLUDED.human_price,
                timestamp = EXCLUDED.timestamp,
                ingested_at = now()
        """
        postgres.execute_many(insert_sql, rows)

    context.log.info(
        f"Inserted {len(rows)} price history records ({errors} errors) into {_SCHEMA}.price_history"
    )

    return MaterializeResult(
        metadata={
            "count": len(rows),
            "errors": errors,
            "listings_processed": len(ids) - errors,
        }
    )
