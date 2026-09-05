"""Shared test fixtures."""

from unittest.mock import MagicMock

from funda import (
    Address,
    Areas,
    Broker,
    GeoLocation,
    Insights,
    Listing,
    Media,
    MediaItem,
    Price,
    PropertyDetails,
    Rooms,
    Urls,
)


def make_mock_engine(select_rows: list[tuple] | None = None):
    """Return a mock SQLAlchemy engine."""
    select_rows = select_rows or []

    engine = MagicMock()

    write_conn = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=write_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    read_conn = MagicMock()
    read_conn.execute.return_value = iter(select_rows)
    engine.connect.return_value.__enter__ = MagicMock(return_value=read_conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    return engine, write_conn, read_conn


def make_mock_listing(data: dict):
    """Build a real pyfunda 3.x Listing from a flat (2.x-style) data dict."""
    features = {
        "has_garden": data.get("has_garden"),
        "has_balcony": data.get("has_balcony"),
        "has_solar_panels": data.get("has_solar_panels"),
        "has_heat_pump": data.get("has_heat_pump"),
        "has_roof_terrace": data.get("has_roof_terrace"),
        "is_energy_efficient": data.get("is_energy_efficient"),
        "is_monument": data.get("is_monument"),
    }
    brokers = (
        (Broker(id=str(data["broker_id"]), name=data.get("broker_name")),)
        if data.get("broker_id")
        else ()
    )
    views = data.get("views")
    saves = data.get("saves")
    return Listing(
        global_id=data.get("global_id"),
        tiny_id=data.get("tiny_id"),
        offering_type=data.get("offering_type"),
        address=Address(
            title=data.get("title"),
            city=data.get("city"),
            postcode=data.get("postcode"),
            province=data.get("province"),
            neighbourhood=data.get("neighbourhood"),
            municipality=data.get("municipality"),
        ),
        price=Price(amount=data.get("price"), formatted=data.get("price_formatted")),
        areas=Areas(living=data.get("living_area"), plot=data.get("plot_area")),
        rooms=Rooms(total=data.get("rooms"), bedrooms=data.get("bedrooms")),
        property_details=PropertyDetails(
            object_type=data.get("object_type"),
            construction_type=data.get("construction_type"),
            construction_year=data.get("construction_year"),
            house_type=data.get("house_type"),
            energy_label=data.get("energy_label"),
            status=data.get("status"),
            features=features,
        ),
        location=GeoLocation(
            latitude=data.get("latitude"), longitude=data.get("longitude")
        ),
        urls=Urls(full=data.get("url")),
        media=Media(
            photos=tuple(
                MediaItem(id=str(i)) for i in range(data.get("photo_count") or 0)
            )
        ),
        brokers=brokers,
        description=data.get("description"),
        publication_date=data.get("publication_date") or data.get("publish_date"),
        insights=(
            Insights(views=views, saves=saves)
            if views is not None or saves is not None
            else None
        ),
    )
