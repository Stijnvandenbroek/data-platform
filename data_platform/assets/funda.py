"""Dagster assets for Funda real-estate data ingestion.

Three assets form a pipeline:

    funda_search_results  →  funda_listing_details  →  funda_price_history

Each asset is configurable from the Dagster launchpad so search
parameters (location, price range, etc.) can be tweaked per run.
"""

import json

from dagster import (
    AssetExecutionContext,
    Config,
    MaterializeResult,
    MetadataValue,
    asset,
)
from sqlalchemy import text

from data_platform.resources import FundaResource, PostgresResource

# ---------------------------------------------------------------------------
# Launchpad config schemas
# ---------------------------------------------------------------------------


class FundaSearchConfig(Config):
    """Launchpad parameters for the Funda search asset."""

    location: str = "amsterdam"
    offering_type: str = "buy"
    price_min: int | None = None
    price_max: int | None = None
    area_min: int | None = None
    area_max: int | None = None
    plot_min: int | None = None
    plot_max: int | None = None
    object_type: str | None = None  # comma-separated, e.g. "house,apartment"
    energy_label: str | None = None  # comma-separated, e.g. "A,A+,A++"
    radius_km: int | None = None
    sort: str = "newest"
    max_pages: int = 3


class FundaDetailsConfig(Config):
    """Launchpad parameters for the listing-details asset."""

    fetch_all: bool = True  # fetch details for every search result


class FundaPriceHistoryConfig(Config):
    """Launchpad parameters for the price-history asset."""

    fetch_all: bool = True  # fetch price history for every detailed listing


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

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
    ingested_at     TIMESTAMPTZ DEFAULT now()
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
    ingested_at         TIMESTAMPTZ DEFAULT now()
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
    ingested_at     TIMESTAMPTZ DEFAULT now()
);
"""


def _safe(val):
    """Convert non-serialisable values (tuples, lists of dicts, etc.) for JSONB."""
    if isinstance(val, list | dict | tuple):
        return json.dumps(val, default=str)
    return val


def _safe_int(val):
    """Try to cast to int, return None on failure."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return None


# ---------------------------------------------------------------------------
# Assets
# ---------------------------------------------------------------------------


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

    # Build search kwargs from launchpad config
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

    # Paginate through results
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

    # Write to Postgres
    engine = postgres.get_engine()
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {_SCHEMA}"))
        conn.execute(text(_DDL_SEARCH))
        # Truncate before inserting fresh results
        conn.execute(text(f"TRUNCATE TABLE {_SCHEMA}.search_results"))

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
                "price": _safe_int(d.get("price")),
                "living_area": _safe_int(d.get("living_area")),
                "plot_area": _safe_int(d.get("plot_area")),
                "bedrooms": _safe_int(d.get("bedrooms")),
                "rooms": _safe_int(d.get("rooms")),
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
                _search_preview_table(rows[:10]),
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

    # Read listing IDs from search results
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT DISTINCT global_id FROM {_SCHEMA}.search_results")
        )
        ids = [row[0] for row in result if row[0]]

    if not ids:
        context.log.warning("No search results found – run funda_search_results first.")
        return MaterializeResult(metadata={"count": 0})

    context.log.info(f"Fetching details for {len(ids)} listings …")

    # Truncate before inserting
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {_SCHEMA}.listing_details"))

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
                    "price": _safe_int(d.get("price")),
                    "price_formatted": d.get("price_formatted"),
                    "status": d.get("status"),
                    "offering_type": d.get("offering_type"),
                    "object_type": d.get("object_type"),
                    "house_type": d.get("house_type"),
                    "construction_type": d.get("construction_type"),
                    "construction_year": d.get("construction_year"),
                    "energy_label": d.get("energy_label"),
                    "living_area": _safe_int(d.get("living_area")),
                    "plot_area": _safe_int(d.get("plot_area")),
                    "bedrooms": _safe_int(d.get("bedrooms")),
                    "rooms": _safe_int(d.get("rooms")),
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
                    "photo_count": _safe_int(d.get("photo_count")),
                    "views": _safe_int(d.get("views")),
                    "saves": _safe_int(d.get("saves")),
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
                _details_preview_table(rows[:10]),
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

    # Read listings from details table
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

    # Truncate before inserting
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {_SCHEMA}.price_history"))

    rows = []
    errors = 0
    for i, gid in enumerate(ids):
        try:
            # get_price_history needs a Listing object, so fetch it first
            listing = client.get_listing(int(gid))
            history = client.get_price_history(listing)
            for entry in history:
                rows.append(
                    {
                        "global_id": gid,
                        "price": _safe_int(entry.get("price")),
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


# ---------------------------------------------------------------------------
# Metadata preview helpers
# ---------------------------------------------------------------------------


def _search_preview_table(rows: list[dict]) -> str:
    """Build a markdown table for search result metadata preview."""
    lines = [
        "| Title | City | Price | Area | Bedrooms |",
        "| --- | --- | --- | --- | --- |",
    ]
    for r in rows:
        price = f"€{r['price']:,}" if r.get("price") else "–"
        area = f"{r['living_area']} m²" if r.get("living_area") else "–"
        lines.append(
            f"| {r.get('title', '–')} | {r.get('city', '–')} "
            f"| {price} | {area} | {r.get('bedrooms', '–')} |"
        )
    return "\n".join(lines)


def _details_preview_table(rows: list[dict]) -> str:
    """Build a markdown table for listing details metadata preview."""
    lines = [
        "| Title | City | Price | Status | Energy |",
        "| --- | --- | --- | --- | --- |",
    ]
    for r in rows:
        price = f"€{r['price']:,}" if r.get("price") else "–"
        lines.append(
            f"| {r.get('title', '–')} | {r.get('city', '–')} "
            f"| {price} | {r.get('status', '–')} | {r.get('energy_label', '–')} |"
        )
    return "\n".join(lines)
