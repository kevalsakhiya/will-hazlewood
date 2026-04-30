from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Optional


@dataclass(slots=True)
class PropertyFinderBrokerItem:
    # --- identity / provenance ---
    platform: str = "propertyfinder"
    scrape_date: Optional[str] = None  # ISO date, UTC
    agent_url: Optional[str] = None
    broker_name: Optional[str] = None
    brn: Optional[str] = None
    nationality: Optional[str] = None
    agent_specialization: Optional[str] = None
    experience_since: Optional[int] = None
    whatsapp_response_time: Optional[int] = None  # seconds
    is_superagent: Optional[bool] = None

    # --- agency ---
    agency_url: Optional[str] = None
    agency_registration_number: Optional[str] = None

    # --- listing counts ---
    listings_for_sale: Optional[int] = None
    listings_for_rent: Optional[int] = None
    listings_total: Optional[int] = None
    listings_with_marketing_spend: Optional[int] = None

    # --- listing prices / ages ---
    average_listing_price_sale: Optional[float] = None
    average_listing_price_rent: Optional[float] = None
    average_listing_age_days_sale: Optional[float] = None
    average_listing_age_days_rent: Optional[float] = None
    most_recent_listing_date_sale: Optional[str] = None
    most_recent_listing_date_rent: Optional[str] = None

    # --- closed transactions / deals ---
    closed_transaction_sale: Optional[int] = None
    closed_transaction_rent: Optional[int] = None
    closed_deals_total: Optional[int] = None
    closed_transaction_deal_value: Optional[float] = None
    closed_transaction_sale_total_amount: Optional[float] = None
    closed_transaction_rent_total_amount: Optional[float] = None
    closed_transaction_sale_avg_amount: Optional[float] = None
    closed_transaction_rent_avg_amount: Optional[float] = None
    most_recent_deal_date_sale: Optional[str] = None
    most_recent_deal_date_rent: Optional[str] = None
    average_monthly_deal_volume_sale: Optional[float] = None
    average_monthly_deal_volume_rent: Optional[float] = None

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
    most_recent_listing_date_rent: Optional[date] = None
    most_recent_listing_date_sale: Optional[date] = None
