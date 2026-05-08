"""Scrapy item dataclasses (the spider-emitted shape).

The two-layer model: spiders produce these flat dataclasses; the
validation pipeline converts them via `to_dict()` and runs
`pydantic` schema validation in `schemas.py`. Default every field
to `None` so spiders only set what they extracted.

`@dataclass(slots=True)` for memory efficiency — Scrapy keeps items in
flight in queues and a single PF run scrapes ~30k brokers.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date


@dataclass(slots=True)
class PropertyFinderBrokerItem:
    # --- identity / provenance ---
    platform: str = "propertyfinder"
    scrape_date: str | None = None  # ISO date, UTC
    agent_url: str | None = None
    broker_name: str | None = None
    brn: str | None = None
    nationality: str | None = None
    agent_specialization: str | None = None
    experience_since: int | None = None
    whatsapp_response_time: int | None = None  # seconds
    is_superagent: bool | None = None

    # --- agency ---
    agency_url: str | None = None
    agency_registration_number: str | None = None

    # --- match / DLD ground truth (populated by Phase 6 matching layer) ---
    match_status: str | None = None         # exact_brn|name_unique|name_fuzzy|ambiguous|not_found|unknown
    match_confidence: float | None = None   # 0..1
    dld_brn: str | None = None              # DLD CardNumber
    dld_broker_name: str | None = None      # DLD CardHolderNameEn (pre-normalization)
    agency_name: str | None = None          # DLD OfficeNameEn

    # --- listing counts ---
    listings_for_sale: int | None = None
    listings_for_rent: int | None = None
    listings_total: int | None = None
    listings_with_marketing_spend: int | None = None

    # --- listing prices / ages ---
    average_listing_price_sale: float | None = None
    average_listing_price_rent: float | None = None
    average_listing_age_days_sale: float | None = None
    average_listing_age_days_rent: float | None = None
    most_recent_listing_date_sale: str | None = None
    most_recent_listing_date_rent: str | None = None

    # --- closed transactions / deals ---
    closed_transaction_sale: int | None = None
    closed_transaction_rent: int | None = None
    closed_deals_total: int | None = None
    closed_transaction_deal_value: float | None = None
    closed_transaction_sale_total_amount: float | None = None
    closed_transaction_rent_total_amount: float | None = None
    closed_transaction_sale_avg_amount: float | None = None
    closed_transaction_rent_avg_amount: float | None = None
    most_recent_deal_date_sale: str | None = None
    most_recent_deal_date_rent: str | None = None
    average_monthly_deal_volume_sale: float | None = None
    average_monthly_deal_volume_rent: float | None = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(slots=True)
class ListingAggState:
    """In-flight aggregates accumulated across paginated listing-API requests."""

    listings_with_marketing_spend: int = 0
    total_property_rent_price: float = 0.0
    total_property_sale_price: float = 0.0
    total_listing_age_days_rent: int = 0
    total_listing_age_days_sale: int = 0
    most_recent_listing_date_rent: date | None = None
    most_recent_listing_date_sale: date | None = None
