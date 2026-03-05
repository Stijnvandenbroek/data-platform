"""Funda ingestion assets."""

from data_platform.assets.ingestion.funda.funda import (
    raw_funda_listing_details,
    raw_funda_price_history,
    raw_funda_search_results,
)

__all__ = [
    "raw_funda_listing_details",
    "raw_funda_price_history",
    "raw_funda_search_results",
]
