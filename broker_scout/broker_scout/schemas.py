"""Pydantic validation schemas for scraped broker items.

Boundary layer between spider output and downstream pipelines: every item
passes through `PropertyFinderBrokerSchema.model_validate(asdict(item))`
in `pipelines/validation.py` (Phase 2.3). Failures are dropped + logged
+ buffered into `spider.bad_items`.

Rules encoded here are the source of truth — roadmap §2.2 lists them in
prose, this module makes them executable.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Annotated, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    field_validator,
    model_validator,
)
from typing_extensions import Literal

# --- bounds (single source of truth; tests in 2.4 import these) ---

MIN_EXPERIENCE_YEAR = 1980
MIN_DATE = date(2000, 1, 1)
MAX_LISTINGS_PER_BUCKET = 5000
MAX_LISTING_AGE_DAYS = 36_500          # 100 years
MAX_AED = 10**9
MAX_RESPONSE_TIME_S = 86_400           # 1 day
SCRAPE_DATE_TOLERANCE_DAYS = 1         # ±1 around today UTC

PF_URL_PREFIX = "https://www.propertyfinder.ae/"

_DATE_FIELDS_PARSED = (
    "scrape_date",
    "most_recent_listing_date_sale",
    "most_recent_listing_date_rent",
    "most_recent_deal_date_sale",
    "most_recent_deal_date_rent",
)

_PAST_DATE_FIELDS = (
    "most_recent_listing_date_sale",
    "most_recent_listing_date_rent",
    "most_recent_deal_date_sale",
    "most_recent_deal_date_rent",
)


# --- helpers ---


def _today_utc() -> date:
    return datetime.now(UTC).date()


def _parse_iso_date(v):
    """`mode='before'` parser: pass through None/date, parse str via `date.fromisoformat`."""
    if v is None or isinstance(v, date):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        return date.fromisoformat(s)
    raise ValueError(f"unsupported date input: {type(v).__name__}")


def _check_past_date_bounds(v: Optional[date]) -> Optional[date]:
    """`mode='after'`: enforce MIN_DATE ≤ v ≤ today UTC."""
    if v is None:
        return v
    if v < MIN_DATE:
        raise ValueError(f"date {v.isoformat()} is before {MIN_DATE.isoformat()}")
    today = _today_utc()
    if v > today:
        raise ValueError(f"date {v.isoformat()} is in the future (today UTC={today.isoformat()})")
    return v


def _check_pf_url(v: Optional[str]) -> Optional[str]:
    if v is None:
        return v
    if not v.startswith(PF_URL_PREFIX):
        raise ValueError(f"url must start with {PF_URL_PREFIX!r}")
    return v


# --- schema ---


class PropertyFinderBrokerSchema(BaseModel):
    """Validates a `PropertyFinderBrokerItem` (as a dict via `asdict`).

    Every field except `platform` is `Optional`. The pipeline serializes
    via `model.model_dump(mode="json")` so dates round-trip back to ISO
    strings for downstream pipelines.
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    # --- identity / provenance ---
    platform: Literal["propertyfinder"]
    scrape_date: Optional[date] = None
    agent_url: Optional[str] = None
    broker_name: Annotated[Optional[str], Field(min_length=1, max_length=200)] = None
    brn: Annotated[Optional[str], Field(min_length=1)] = None
    nationality: Annotated[Optional[str], Field(max_length=100)] = None
    agent_specialization: Annotated[Optional[str], Field(max_length=100)] = None
    experience_since: Optional[int] = None
    whatsapp_response_time: Annotated[
        Optional[int], Field(ge=0, le=MAX_RESPONSE_TIME_S)
    ] = None
    is_superagent: Optional[StrictBool] = None

    # --- agency ---
    agency_url: Optional[str] = None
    agency_registration_number: Annotated[
        Optional[str], Field(min_length=1, max_length=100)
    ] = None

    # --- listing counts ---
    listings_for_sale: Annotated[
        Optional[int], Field(ge=0, le=MAX_LISTINGS_PER_BUCKET)
    ] = None
    listings_for_rent: Annotated[
        Optional[int], Field(ge=0, le=MAX_LISTINGS_PER_BUCKET)
    ] = None
    listings_total: Annotated[Optional[int], Field(ge=0)] = None
    listings_with_marketing_spend: Annotated[Optional[int], Field(ge=0)] = None

    # --- listing prices / ages ---
    average_listing_price_sale: Annotated[Optional[float], Field(ge=0, le=MAX_AED)] = None
    average_listing_price_rent: Annotated[Optional[float], Field(ge=0, le=MAX_AED)] = None
    average_listing_age_days_sale: Annotated[
        Optional[float], Field(ge=0, le=MAX_LISTING_AGE_DAYS)
    ] = None
    average_listing_age_days_rent: Annotated[
        Optional[float], Field(ge=0, le=MAX_LISTING_AGE_DAYS)
    ] = None
    most_recent_listing_date_sale: Optional[date] = None
    most_recent_listing_date_rent: Optional[date] = None

    # --- closed transactions / deals ---
    closed_transaction_sale: Annotated[Optional[int], Field(ge=0)] = None
    closed_transaction_rent: Annotated[Optional[int], Field(ge=0)] = None
    closed_deals_total: Annotated[Optional[int], Field(ge=0)] = None
    closed_transaction_deal_value: Annotated[
        Optional[float], Field(ge=0, le=MAX_AED)
    ] = None
    closed_transaction_sale_total_amount: Annotated[
        Optional[float], Field(ge=0, le=MAX_AED)
    ] = None
    closed_transaction_rent_total_amount: Annotated[
        Optional[float], Field(ge=0, le=MAX_AED)
    ] = None
    closed_transaction_sale_avg_amount: Annotated[
        Optional[float], Field(ge=0, le=MAX_AED)
    ] = None
    closed_transaction_rent_avg_amount: Annotated[
        Optional[float], Field(ge=0, le=MAX_AED)
    ] = None
    most_recent_deal_date_sale: Optional[date] = None
    most_recent_deal_date_rent: Optional[date] = None
    average_monthly_deal_volume_sale: Annotated[Optional[float], Field(ge=0)] = None
    average_monthly_deal_volume_rent: Annotated[Optional[float], Field(ge=0)] = None

    # --- field-level validators ---

    @field_validator(*_DATE_FIELDS_PARSED, mode="before")
    @classmethod
    def _parse_dates(cls, v):
        return _parse_iso_date(v)

    @field_validator(*_PAST_DATE_FIELDS, mode="after")
    @classmethod
    def _enforce_past_date(cls, v):
        return _check_past_date_bounds(v)

    @field_validator("scrape_date", mode="after")
    @classmethod
    def _check_scrape_date_window(cls, v: Optional[date]) -> Optional[date]:
        if v is None:
            return v
        today = _today_utc()
        if abs((v - today).days) > SCRAPE_DATE_TOLERANCE_DAYS:
            raise ValueError(
                f"scrape_date {v.isoformat()} is outside ±{SCRAPE_DATE_TOLERANCE_DAYS}d "
                f"of today UTC {today.isoformat()}"
            )
        return v

    @field_validator("experience_since", mode="after")
    @classmethod
    def _check_experience_year(cls, v: Optional[int]) -> Optional[int]:
        if v is None:
            return v
        current_year = _today_utc().year
        if v < MIN_EXPERIENCE_YEAR or v > current_year:
            raise ValueError(
                f"experience_since must be {MIN_EXPERIENCE_YEAR}..{current_year}, got {v}"
            )
        return v

    @field_validator("agent_url", "agency_url", mode="after")
    @classmethod
    def _check_pf_urls(cls, v: Optional[str]) -> Optional[str]:
        return _check_pf_url(v)

    # --- cross-field validators ---

    @model_validator(mode="after")
    def _check_listings_total(self) -> "PropertyFinderBrokerSchema":
        sale, rent, total = (
            self.listings_for_sale,
            self.listings_for_rent,
            self.listings_total,
        )
        if sale is None and rent is None:
            if total is not None:
                raise ValueError(
                    "listings_total must be null when both listings_for_sale and "
                    "listings_for_rent are null"
                )
        else:
            expected = (sale or 0) + (rent or 0)
            if total != expected:
                raise ValueError(
                    f"listings_total ({total}) must equal "
                    f"listings_for_sale ({sale}) + listings_for_rent ({rent}) = {expected}"
                )
        return self

    @model_validator(mode="after")
    def _check_closed_deals_total(self) -> "PropertyFinderBrokerSchema":
        sale, rent, total = (
            self.closed_transaction_sale,
            self.closed_transaction_rent,
            self.closed_deals_total,
        )
        if sale is None and rent is None:
            if total is not None:
                raise ValueError(
                    "closed_deals_total must be null when both closed_transaction_sale "
                    "and closed_transaction_rent are null"
                )
        else:
            expected = (sale or 0) + (rent or 0)
            if total != expected:
                raise ValueError(
                    f"closed_deals_total ({total}) must equal "
                    f"closed_transaction_sale ({sale}) + closed_transaction_rent ({rent}) "
                    f"= {expected}"
                )
        return self

    @model_validator(mode="after")
    def _check_marketing_spend_bound(self) -> "PropertyFinderBrokerSchema":
        spend, total = self.listings_with_marketing_spend, self.listings_total
        if spend is not None and total is not None and spend > total:
            raise ValueError(
                f"listings_with_marketing_spend ({spend}) must be ≤ listings_total ({total})"
            )
        return self


__all__ = ["PropertyFinderBrokerSchema"]
