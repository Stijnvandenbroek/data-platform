"""Tests for Funda assets."""

from unittest.mock import MagicMock

from dagster import materialize

from data_platform.assets.ingestion.funda import (
    raw_funda_listing_details,
    raw_funda_price_history,
    raw_funda_search_results,
)
from data_platform.assets.ingestion.funda.funda import FundaSearchConfig
from tests.conftest import make_mock_engine, make_mock_listing


class MockFundaResource:
    """Test double for FundaResource."""

    def __init__(self, client):
        self._client = client

    def get_client(self):
        return self._client


class MockPostgresResource:
    """Test double for PostgresResource."""

    def __init__(self, engine=None, inserted_rows: list | None = None):
        self._engine = engine or make_mock_engine()[0]
        self._inserted_rows = inserted_rows if inserted_rows is not None else []

    def get_engine(self):
        return self._engine

    def execute(self, statement, params=None):
        pass

    def execute_many(self, statement, rows):
        self._inserted_rows.extend(rows)


_SEARCH_LISTING_DATA = {
    "global_id": "1234567",
    "title": "Teststraat 1",
    "city": "Amsterdam",
    "postcode": "1234AB",
    "province": "Noord-Holland",
    "neighbourhood": "Centrum",
    "price": 350000,
    "living_area": 80,
    "plot_area": None,
    "bedrooms": 3,
    "rooms": 5,
    "energy_label": "A",
    "object_type": "apartment",
    "offering_type": "buy",
    "construction_type": "existing",
    "publish_date": "2026-01-15",
    "broker_id": "999",
    "broker_name": "Test Makelaars",
}

_DETAIL_LISTING_DATA = {
    **_SEARCH_LISTING_DATA,
    "tiny_id": "87654321",
    "municipality": "Amsterdam",
    "price_formatted": "\u20ac 350.000 k.k.",
    "status": "available",
    "house_type": "Appartement",
    "construction_year": "1985",
    "description": "A lovely apartment.",
    "publication_date": "2026-01-15",
    "latitude": 52.37,
    "longitude": 4.89,
    "has_garden": False,
    "has_balcony": True,
    "has_solar_panels": False,
    "has_heat_pump": False,
    "has_roof_terrace": False,
    "is_energy_efficient": True,
    "is_monument": False,
    "url": "https://www.funda.nl/detail/koop/amsterdam/app/87654321/",
    "photo_count": 12,
    "views": 150,
    "saves": 30,
}


class TestFundaSearchResults:
    def _run(self, mock_client, inserted_rows=None, config=None):
        engine, _, _ = make_mock_engine()
        rows = inserted_rows if inserted_rows is not None else []
        result = materialize(
            [raw_funda_search_results],
            resources={
                "funda": MockFundaResource(mock_client),
                "postgres": MockPostgresResource(engine, rows),
            },
            run_config={
                "ops": {
                    "raw_funda_search_results": {
                        "config": {"max_pages": 1, **(config or {})}
                    }
                }
            },
        )
        return result

    def test_no_results_returns_count_zero(self):
        client = MagicMock()
        client.search_listing.return_value = []
        result = self._run(client)
        assert result.success
        mat = result.asset_materializations_for_node("raw_funda_search_results")
        assert mat[0].metadata["count"].value == 0

    def test_results_are_inserted(self):
        client = MagicMock()
        client.search_listing.return_value = [make_mock_listing(_SEARCH_LISTING_DATA)]
        rows = []
        result = self._run(client, inserted_rows=rows)
        assert result.success
        assert len(rows) == 1
        assert rows[0]["city"] == "Amsterdam"
        assert rows[0]["price"] == 350000

    def test_pagination_stops_on_empty_page(self):
        client = MagicMock()
        client.search_listing.side_effect = [
            [make_mock_listing(_SEARCH_LISTING_DATA)],
            [],
        ]
        inserted = []
        result = materialize(
            [raw_funda_search_results],
            resources={
                "funda": MockFundaResource(client),
                "postgres": MockPostgresResource(make_mock_engine()[0], inserted),
            },
            run_config={
                "ops": {"raw_funda_search_results": {"config": {"max_pages": 3}}}
            },
        )
        assert result.success
        assert client.search_listing.call_count == 2
        assert len(inserted) == 1

    def test_location_split_by_comma(self):
        client = MagicMock()
        client.search_listing.return_value = []
        self._run(client, config={"location": "amsterdam, rotterdam"})
        call_kwargs = client.search_listing.call_args[1]
        assert call_kwargs["location"] == ["amsterdam", "rotterdam"]

    def test_price_max_forwarded(self):
        client = MagicMock()
        client.search_listing.return_value = []
        self._run(client, config={"price_max": 500000})
        assert client.search_listing.call_args[1]["price_max"] == 500000

    def test_price_min_forwarded(self):
        client = MagicMock()
        client.search_listing.return_value = []
        self._run(client, config={"price_min": 200000})
        assert client.search_listing.call_args[1]["price_min"] == 200000

    def test_area_min_forwarded(self):
        client = MagicMock()
        client.search_listing.return_value = []
        self._run(client, config={"area_min": 50})
        assert client.search_listing.call_args[1]["area_min"] == 50

    def test_radius_km_forwarded(self):
        client = MagicMock()
        client.search_listing.return_value = []
        self._run(client, config={"location": "1012AB", "radius_km": 10})
        assert client.search_listing.call_args[1]["radius_km"] == 10

    def test_object_type_split_by_comma(self):
        client = MagicMock()
        client.search_listing.return_value = []
        self._run(client, config={"object_type": "house, apartment"})
        assert client.search_listing.call_args[1]["object_type"] == [
            "house",
            "apartment",
        ]

    def test_energy_label_split_by_comma(self):
        client = MagicMock()
        client.search_listing.return_value = []
        self._run(client, config={"energy_label": "A, A+"})
        assert client.search_listing.call_args[1]["energy_label"] == ["A", "A+"]


class TestFundaListingDetails:
    def _run(self, mock_client, engine, inserted_rows=None):
        rows = inserted_rows if inserted_rows is not None else []
        return materialize(
            [raw_funda_listing_details],
            resources={
                "funda": MockFundaResource(mock_client),
                "postgres": MockPostgresResource(engine, rows),
            },
        )

    def test_no_search_results_returns_count_zero(self):
        engine, _, _ = make_mock_engine(select_rows=[])
        client = MagicMock()
        result = self._run(client, engine)
        assert result.success
        mat = result.asset_materializations_for_node("raw_funda_listing_details")
        assert mat[0].metadata["count"].value == 0

    def test_details_fetched_and_inserted(self):
        engine, _, _ = make_mock_engine(select_rows=[("1234567",)])
        client = MagicMock()
        client.get_listing.return_value = make_mock_listing(_DETAIL_LISTING_DATA)
        inserted = []
        result = self._run(client, engine, inserted)
        assert result.success
        assert len(inserted) == 1
        assert inserted[0]["city"] == "Amsterdam"
        assert inserted[0]["status"] == "available"
        assert inserted[0]["has_balcony"] is True
        assert inserted[0]["has_garden"] is False

    def test_failed_fetch_counted_as_error(self):
        engine, _, _ = make_mock_engine(select_rows=[("1234567",), ("9999999",)])
        client = MagicMock()
        client.get_listing.side_effect = [
            make_mock_listing(_DETAIL_LISTING_DATA),
            RuntimeError("API error"),
        ]
        inserted = []
        result = self._run(client, engine, inserted)
        assert result.success
        mat = result.asset_materializations_for_node("raw_funda_listing_details")
        assert mat[0].metadata["errors"].value == 1
        assert len(inserted) == 1


class TestFundaPriceHistory:
    def _run(self, mock_client, engine, inserted_rows=None):
        rows = inserted_rows if inserted_rows is not None else []
        return materialize(
            [raw_funda_price_history],
            resources={
                "funda": MockFundaResource(mock_client),
                "postgres": MockPostgresResource(engine, rows),
            },
        )

    def test_no_details_returns_count_zero(self):
        engine, _, _ = make_mock_engine(select_rows=[])
        client = MagicMock()
        result = self._run(client, engine)
        assert result.success
        mat = result.asset_materializations_for_node("raw_funda_price_history")
        assert mat[0].metadata["count"].value == 0

    def test_price_history_inserted(self):
        engine, _, _ = make_mock_engine(
            select_rows=[
                (
                    "1234567",
                    "https://www.funda.nl/detail/koop/amsterdam/app/87654321/",
                    "Teststraat 1",
                    "1234AB",
                ),
            ]
        )
        client = MagicMock()
        client.get_price_history.return_value = [
            {
                "price": 350000,
                "human_price": "\u20ac350.000",
                "date": "1 jan, 2026",
                "timestamp": "2026-01-01T00:00:00",
                "source": "Funda",
                "status": "asking_price",
            },
            {
                "price": 320000,
                "human_price": "\u20ac320.000",
                "date": "1 jan, 2024",
                "timestamp": "2024-01-01T00:00:00",
                "source": "WOZ",
                "status": "woz",
            },
        ]
        inserted = []
        result = self._run(client, engine, inserted)
        assert result.success
        assert len(inserted) == 2
        assert inserted[0]["source"] == "Funda"
        assert inserted[1]["source"] == "WOZ"
        mat = result.asset_materializations_for_node("raw_funda_price_history")
        assert mat[0].metadata["count"].value == 2


class TestFundaSearchConfig:
    def test_defaults(self):
        cfg = FundaSearchConfig()
        assert cfg.location == "woerden"
        assert cfg.offering_type == "buy"
        assert cfg.sort == "newest"
        assert cfg.max_pages == 10
        assert cfg.price_min == 300000
        assert cfg.price_max == 500000

    def test_custom_values(self):
        cfg = FundaSearchConfig(
            location="rotterdam",
            offering_type="rent",
            price_max=2000,
            max_pages=1,
        )
        assert cfg.location == "rotterdam"
        assert cfg.offering_type == "rent"
        assert cfg.price_max == 2000
