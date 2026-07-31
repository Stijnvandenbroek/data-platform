"""Regression tests for the pyfunda 3.x API migration of the Funda assets.

These tests exercise the *real* pyfunda 3.1.4 client (with a mocked HTTP
transport) to verify the asset uses the current supported API and template
instead of the retired ``search_listing`` method / ``search_result_20250805``
template.
"""

import inspect
from unittest.mock import MagicMock

from dagster import materialize
from funda import Funda
from funda.constants import SEARCH_TEMPLATE_ID

from data_platform.assets.ingestion.funda import raw_funda_search_results
from data_platform.assets.ingestion.funda import funda as funda_asset_module
from tests.conftest import make_mock_engine

_RETIRED_TEMPLATE_ID = "search_result_20250805"

# Minimal shape of a current search-template hit as parsed by pyfunda 3.1.4.
_SEARCH_HIT = {
    "_id": "1234567",
    "_source": {
        "id": 1234567,
        "object_detail_page_relative_url": "/koop/amsterdam/appartement-1234567/",
        "offering_type": ["koop"],
        "address": {
            "title": "Teststraat 1",
            "street_name": "Teststraat",
            "house_number": "1",
            "postal_code": "1234AB",
            "city": "Amsterdam",
            "municipality": "Amsterdam",
            "neighbourhood": "Centrum",
            "province": "Noord-Holland",
        },
        "price": {"selling_price": [350000]},
        "floor_area": [80],
        "number_of_rooms": 5,
        "number_of_bedrooms": 3,
        "object_type": ["appartement"],
        "construction_type": ["bestaande bouw"],
        "energy_label": "A",
        "publish_date": "2026-01-15",
        "agent": [{"id": "999", "name": "Test Makelaars"}],
        "thumbnail_id": ["thumb-1"],
    },
}


class _FakeFundaResource:
    def __init__(self, client):
        self._client = client

    def get_client(self):
        return self._client


class _FakePostgresResource:
    def __init__(self, engine, inserted_rows):
        self._engine = engine
        self._inserted_rows = inserted_rows

    def get_engine(self):
        return self._engine

    def execute(self, statement, params=None):
        pass

    def execute_many(self, statement, rows):
        self._inserted_rows.extend(rows)


def _mock_search_response():
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"responses": [{"hits": {"hits": [_SEARCH_HIT]}}]}
    return response


def test_asset_module_does_not_reference_obsolete_api():
    source = inspect.getsource(funda_asset_module)
    assert "search_listing" not in source
    assert "get_listing" not in source
    assert "get_price_history" not in source
    assert "client.search(" in source
    assert "client.listing(" in source
    assert "client.price_history(" in source


def test_current_template_id_is_used_by_pyfunda():
    assert SEARCH_TEMPLATE_ID == "search_result_20260227"


def test_search_uses_current_template_not_retired_one():
    client = Funda()
    client._transport = MagicMock()
    client._transport.post.return_value = _mock_search_response()

    assert not hasattr(client, "search_listing")

    listings = client.search(
        ["woerden"],
        category="buy",
        sort="newest",
        min_price=300000,
        max_price=500000,
        page=0,
    )
    assert len(listings) == 1

    payload = client._transport.post.call_args[1]["data"]
    assert SEARCH_TEMPLATE_ID in payload
    assert _RETIRED_TEMPLATE_ID not in payload


def test_search_asset_works_end_to_end_with_real_pyfunda_client():
    client = Funda()
    client._transport = MagicMock()
    client._transport.post.return_value = _mock_search_response()

    inserted = []
    result = materialize(
        [raw_funda_search_results],
        resources={
            "funda": _FakeFundaResource(client),
            "postgres": _FakePostgresResource(make_mock_engine()[0], inserted),
        },
        run_config={
            "ops": {
                "raw_funda_search_results": {
                    "config": {"location": "woerden", "max_pages": 1}
                }
            }
        },
    )

    assert result.success
    assert len(inserted) == 1
    assert inserted[0]["city"] == "Amsterdam"
    assert inserted[0]["price"] == 350000
    assert inserted[0]["global_id"] == 1234567

    assert not hasattr(client, "search_listing")
    assert client._transport.post.call_count == 1

    payload = client._transport.post.call_args[1]["data"]
    assert SEARCH_TEMPLATE_ID in payload
    assert _RETIRED_TEMPLATE_ID not in payload
