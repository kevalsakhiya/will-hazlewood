"""Coverage for `PropertyFinderBrokerSchema` rules — both rejections and
boundary acceptances. Constants are imported from `schemas` so tightening
a bound auto-tightens the test."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from broker_scout.schemas import (
    MATCH_STATUSES,
    MAX_AED,
    MAX_LISTING_AGE_DAYS,
    MAX_LISTINGS_PER_BUCKET,
    MAX_RESPONSE_TIME_S,
    MIN_DATE,
    MIN_EXPERIENCE_YEAR,
    PropertyFinderBrokerSchema,
)


def _today_utc():
    return datetime.now(UTC).date()


# ---------------------------------------------------------------- happy paths


def test_minimal_payload_passes(make_item):
    """An item with only `platform` set (everything else null) must validate."""
    PropertyFinderBrokerSchema.model_validate(make_item())


def test_fully_populated_payload_passes(make_item):
    today = _today_utc()
    payload = make_item(
        match_status="exact_brn",
        match_confidence=0.95,
        dld_brn="81462",
        dld_broker_name="Dharam Vir Juneja",
        agency_name="DVJ Real Estate L.L.C",
        scrape_date=today.isoformat(),
        agent_url="https://www.propertyfinder.ae/en/agent/foo",
        agency_url="https://www.propertyfinder.ae/en/broker/bar",
        broker_name="Test Broker",
        brn="12345",
        nationality="AE",
        agent_specialization="Sales",
        experience_since=2010,
        whatsapp_response_time=120,
        is_superagent=True,
        agency_registration_number="ABC-1",
        listings_for_sale=2,
        listings_for_rent=3,
        listings_total=5,
        listings_with_marketing_spend=2,
        average_listing_price_sale=1_500_000.0,
        average_listing_price_rent=80_000.0,
        average_listing_age_days_sale=30.0,
        average_listing_age_days_rent=15.0,
        most_recent_listing_date_sale="2025-01-15",
        most_recent_listing_date_rent="2025-02-20",
        closed_transaction_sale=1,
        closed_transaction_rent=0,
        closed_deals_total=1,
        closed_transaction_deal_value=2_000_000.0,
        closed_transaction_sale_total_amount=2_000_000.0,
        closed_transaction_rent_total_amount=0.0,
        closed_transaction_sale_avg_amount=2_000_000.0,
        closed_transaction_rent_avg_amount=0.0,
        most_recent_deal_date_sale="2025-03-01",
        most_recent_deal_date_rent="2025-03-02",
        average_monthly_deal_volume_sale=0.5,
        average_monthly_deal_volume_rent=0.0,
    )
    PropertyFinderBrokerSchema.model_validate(payload)


# ----------------------------------------------------------- field rejections

_CURRENT_YEAR = _today_utc().year

REJECTION_CASES = [
    # URL prefix
    ("agent_url", "https://example.com/x"),
    ("agency_url", "http://www.propertyfinder.ae/x"),  # http, not https
    # Strings
    ("broker_name", ""),
    ("broker_name", "x" * 201),
    ("brn", ""),
    ("nationality", "x" * 101),
    ("agent_specialization", "x" * 101),
    ("agency_registration_number", ""),
    ("agency_registration_number", "x" * 101),
    # Year
    ("experience_since", MIN_EXPERIENCE_YEAR - 1),
    ("experience_since", _CURRENT_YEAR + 1),
    # Bounded ints
    ("whatsapp_response_time", -1),
    ("whatsapp_response_time", MAX_RESPONSE_TIME_S + 1),
    ("listings_for_sale", -1),
    ("listings_for_sale", MAX_LISTINGS_PER_BUCKET + 1),
    ("listings_for_rent", MAX_LISTINGS_PER_BUCKET + 1),
    ("listings_with_marketing_spend", -1),
    # Bounded floats — AED ceiling
    ("average_listing_price_sale", -0.01),
    ("average_listing_price_sale", float(MAX_AED) + 1.0),
    ("average_listing_price_rent", float(MAX_AED) + 1.0),
    ("closed_transaction_deal_value", float(MAX_AED) + 1.0),
    ("closed_transaction_sale_total_amount", float(MAX_AED) + 1.0),
    ("closed_transaction_rent_total_amount", -0.01),
    ("closed_transaction_sale_avg_amount", float(MAX_AED) + 1.0),
    ("closed_transaction_rent_avg_amount", -0.01),
    # Listing-age ceiling
    ("average_listing_age_days_sale", -1),
    ("average_listing_age_days_sale", MAX_LISTING_AGE_DAYS + 1),
    ("average_listing_age_days_rent", MAX_LISTING_AGE_DAYS + 1),
    # Closed transaction counts
    ("closed_transaction_sale", -1),
    ("closed_transaction_rent", -1),
    # Monthly deal volume
    ("average_monthly_deal_volume_sale", -0.01),
    ("average_monthly_deal_volume_rent", -0.01),
    # StrictBool
    ("is_superagent", 1),
    ("is_superagent", 0),
    ("is_superagent", "true"),
    # Dates: bad string, < MIN_DATE, future
    ("most_recent_listing_date_sale", "not-a-date"),
    ("most_recent_listing_date_sale", "1999-12-31"),
    ("most_recent_listing_date_rent", "2099-01-01"),
    ("most_recent_deal_date_sale", "1999-12-31"),
    ("most_recent_deal_date_rent", "2099-01-01"),
    # Phase 6.1: match / DLD ground truth
    ("match_status", "totally_made_up"),       # not in Literal set
    ("match_status", ""),                       # empty string also rejected
    ("match_confidence", -0.01),
    ("match_confidence", 1.01),
    ("dld_brn", ""),
    ("dld_broker_name", ""),
    ("dld_broker_name", "x" * 201),
    ("agency_name", ""),
    ("agency_name", "x" * 201),
]


@pytest.mark.parametrize(
    "field,bad",
    REJECTION_CASES,
    ids=[f"{f}={v!r}" for f, v in REJECTION_CASES],
)
def test_field_level_rejection(make_item, field, bad):
    payload = make_item(**{field: bad})
    with pytest.raises(ValidationError) as exc_info:
        PropertyFinderBrokerSchema.model_validate(payload)
    locs = [".".join(str(p) for p in err["loc"]) for err in exc_info.value.errors()]
    assert any(field in loc for loc in locs), (
        f"expected error loc containing {field!r}, got {locs}"
    )


# --------------------------------------------------------- boundary accepts

ACCEPTANCE_CASES = [
    ("whatsapp_response_time", 0),
    ("whatsapp_response_time", MAX_RESPONSE_TIME_S),
    ("experience_since", MIN_EXPERIENCE_YEAR),
    ("experience_since", _CURRENT_YEAR),
    ("most_recent_listing_date_sale", MIN_DATE.isoformat()),
    ("most_recent_listing_date_rent", _today_utc().isoformat()),
    ("most_recent_deal_date_sale", MIN_DATE.isoformat()),
    ("average_listing_age_days_sale", 0),
    ("average_listing_age_days_sale", MAX_LISTING_AGE_DAYS),
    ("average_listing_price_sale", 0.0),
    ("average_listing_price_sale", float(MAX_AED)),
    ("is_superagent", True),
    ("is_superagent", False),
    ("brn", "1"),
    ("agency_registration_number", "1"),
    # Phase 6.1: every Literal status is acceptable
    ("match_status", "exact_brn"),
    ("match_status", "name_unique"),
    ("match_status", "name_fuzzy"),
    ("match_status", "ambiguous"),
    ("match_status", "not_found"),
    ("match_status", "unknown"),
    # Confidence boundaries
    ("match_confidence", 0.0),
    ("match_confidence", 1.0),
    ("dld_brn", "1"),
    ("dld_broker_name", "X"),
    ("agency_name", "X"),
]


def test_all_literal_match_statuses_have_acceptance_rows():
    """Catch silent drift: if MATCH_STATUSES grows, the parametrize
    table must grow too."""
    accepted = {v for f, v in ACCEPTANCE_CASES if f == "match_status"}
    assert accepted == set(MATCH_STATUSES), (
        f"missing acceptance rows for: {set(MATCH_STATUSES) - accepted}"
    )


@pytest.mark.parametrize(
    "field,good",
    ACCEPTANCE_CASES,
    ids=[f"{f}={v!r}" for f, v in ACCEPTANCE_CASES],
)
def test_field_level_acceptance_at_boundary(make_item, field, good):
    PropertyFinderBrokerSchema.model_validate(make_item(**{field: good}))


@pytest.mark.parametrize("count", [0, MAX_LISTINGS_PER_BUCKET])
def test_listings_bucket_boundary_accepted(make_item, count):
    """Listings buckets at min/max — paired with a matching total to satisfy
    the cross-field rule."""
    PropertyFinderBrokerSchema.model_validate(
        make_item(
            listings_for_sale=count,
            listings_for_rent=0,
            listings_total=count,
        )
    )


# ------------------------------------------------------------- cross-field


def test_listings_total_mismatch_rejected(make_item):
    with pytest.raises(ValidationError):
        PropertyFinderBrokerSchema.model_validate(
            make_item(listings_for_sale=2, listings_for_rent=3, listings_total=99)
        )


def test_listings_total_must_be_null_when_inputs_null(make_item):
    with pytest.raises(ValidationError):
        PropertyFinderBrokerSchema.model_validate(make_item(listings_total=5))


def test_listings_total_null_when_inputs_null_passes(make_item):
    PropertyFinderBrokerSchema.model_validate(make_item())


def test_listings_total_with_one_null_input_treats_as_zero(make_item):
    PropertyFinderBrokerSchema.model_validate(
        make_item(listings_for_sale=2, listings_for_rent=None, listings_total=2)
    )


def test_closed_deals_total_mismatch_rejected(make_item):
    with pytest.raises(ValidationError):
        PropertyFinderBrokerSchema.model_validate(
            make_item(
                closed_transaction_sale=2,
                closed_transaction_rent=2,
                closed_deals_total=99,
            )
        )


def test_closed_deals_total_must_be_null_when_inputs_null(make_item):
    with pytest.raises(ValidationError):
        PropertyFinderBrokerSchema.model_validate(make_item(closed_deals_total=1))


def test_closed_deals_total_with_one_null_input_treats_as_zero(make_item):
    PropertyFinderBrokerSchema.model_validate(
        make_item(
            closed_transaction_sale=3,
            closed_transaction_rent=None,
            closed_deals_total=3,
        )
    )


def test_marketing_spend_exceeds_total_rejected(make_item):
    with pytest.raises(ValidationError):
        PropertyFinderBrokerSchema.model_validate(
            make_item(
                listings_for_sale=2,
                listings_for_rent=3,
                listings_total=5,
                listings_with_marketing_spend=99,
            )
        )


def test_marketing_spend_equal_to_total_accepted(make_item):
    PropertyFinderBrokerSchema.model_validate(
        make_item(
            listings_for_sale=2,
            listings_for_rent=3,
            listings_total=5,
            listings_with_marketing_spend=5,
        )
    )


def test_marketing_spend_ignored_when_total_null(make_item):
    PropertyFinderBrokerSchema.model_validate(
        make_item(listings_with_marketing_spend=10)
    )


# ---------------------------------------------------------- schema guards


def test_extra_key_rejected():
    payload = {"platform": "propertyfinder", "unknown_field": "x"}
    with pytest.raises(ValidationError):
        PropertyFinderBrokerSchema.model_validate(payload)


def test_platform_required():
    with pytest.raises(ValidationError):
        PropertyFinderBrokerSchema.model_validate({})


def test_platform_must_be_propertyfinder(make_item):
    with pytest.raises(ValidationError):
        payload = make_item()
        payload["platform"] = "bayut"
        PropertyFinderBrokerSchema.model_validate(payload)


# ---------------------------------------------------------- scrape_date window


def test_scrape_date_today_accepted(make_item):
    PropertyFinderBrokerSchema.model_validate(
        make_item(scrape_date=_today_utc().isoformat())
    )


@pytest.mark.parametrize("delta_days", [-1, 1])
def test_scrape_date_within_one_day_accepted(make_item, delta_days):
    d = (_today_utc() + timedelta(days=delta_days)).isoformat()
    PropertyFinderBrokerSchema.model_validate(make_item(scrape_date=d))


@pytest.mark.parametrize("delta_days", [-2, 2])
def test_scrape_date_outside_window_rejected(make_item, delta_days):
    d = (_today_utc() + timedelta(days=delta_days)).isoformat()
    with pytest.raises(ValidationError):
        PropertyFinderBrokerSchema.model_validate(make_item(scrape_date=d))


def test_scrape_date_far_past_rejected(make_item):
    with pytest.raises(ValidationError):
        PropertyFinderBrokerSchema.model_validate(make_item(scrape_date="2020-01-01"))
