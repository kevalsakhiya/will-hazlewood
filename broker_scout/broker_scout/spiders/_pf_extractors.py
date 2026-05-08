"""PropertyFinder payload extractors — pure JSON-shape transforms.

Sibling helpers for [agent_spider.py](agent_spider.py). Every function
here takes a parsed PF response (or the agent_data sub-dict) and
populates fields on a `PropertyFinderBrokerItem` (or returns derived
data). They issue no requests and hold no callback state — moving them
out of the spider class keeps `agent_spider.py` focused on routing
and lifecycle.

The two extractors that fall back to HTML (`extract_profile_brn`,
`extract_candidates`) take an optional `stats` collector so they can
record `extract/*` counters when the JSON path fails. Tests pass `None`
to skip the increment without altering the result.

Naming: leading underscore on the *module* (`_pf_extractors`) signals
"private to the PF spider — Bayut should not import this." Function
names are public so the spider's call sites stay readable.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import jmespath

from broker_scout.common.matching import Candidate
from broker_scout.items import ListingAggState, PropertyFinderBrokerItem

if TYPE_CHECKING:  # only for type hints — avoids importing scrapy at runtime here
    from scrapy.http import Response
    from scrapy.statscollectors import StatsCollector

LISTING_API_HEADERS = {
    "accept": "*/*",
    "accept-language": "en-GB,en;q=0.9,en-US;q=0.8,hi;q=0.7,fr;q=0.6,gu;q=0.5",
    "locale": "en",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
    ),
}

LISTING_PAGE_SIZE = 50


# ---------------------------------------------------------------- pure helpers


def max_date(current, candidate):
    """Return the later of two date-likes, treating None as 'no value'."""
    if candidate is None:
        return current
    if current is None or candidate > current:
        return candidate
    return current


def listings_url(agent_id: str | int, page: int) -> str:
    """Build the agent's paginated listings-API URL for one page."""
    return (
        "https://www.propertyfinder.ae/api/pwa/property/search"
        f"?sorting.sort=featured&filters.furnished=all"
        f"&pagination.limit={LISTING_PAGE_SIZE}&pagination.page={page}"
        f"&filters.utilities_price_type=notSelected"
        f"&filters.price_type=price_type_any"
        f"&filters.agent_id={agent_id}&locale=en"
    )


def _inc(stats: StatsCollector | None, key: str) -> None:
    if stats is not None:
        stats.inc_value(key)


# -------------------------------------------------------------- profile / search


def extract_profile_brn(
    response: Response, stats: StatsCollector | None = None
) -> str | None:
    """Pull the BRN out of an agent profile page.

    Tries the `__NEXT_DATA__` JSON first; falls back to the HTML
    'Dubai Broker License' table cell. The fallback path increments
    `extract/brn/fallback_used` so Phase 9 monitors can detect drift.

    Used by both `extract_basic` (matched-flow path) and
    `parse_disambiguating_profile` (the ambiguous walk).
    """
    raw = response.xpath('.//script[@id="__NEXT_DATA__"]/text()').get()
    if raw:
        try:
            next_data = json.loads(raw)
            agent_data = jmespath.search("props.pageProps.agent", next_data) or {}
            brn = jmespath.search("compliances[-1].value", agent_data)
            if brn is not None:
                s = str(brn).strip()
                if s:
                    return s
        except (json.JSONDecodeError, TypeError):
            pass
    fallback = response.xpath(
        './/td[contains(text(),"Dubai Broker License")]'
        "/following-sibling::td/text()"
    ).get()
    if fallback and fallback.strip():
        _inc(stats, "extract/brn/fallback_used")
        return fallback.strip()
    return None


def extract_candidates(
    response: Response, stats: StatsCollector | None = None
) -> list[Candidate]:
    """Pull (name, url, brn) tuples out of the search results.

    Primary source: `props.pageProps.agents.data` from the embedded
    `__NEXT_DATA__` JSON. That payload exposes each candidate's BRN
    directly (in `compliances[?type=='brn'].value`), letting us do
    an exact-BRN match in `match_candidates` without any profile
    fetch. Falls back to HTML XPath when the JSON is missing or
    malformed (defense-in-depth — Phase 9 monitor counts the
    fallback rate via `extract/search_json/fallback_used`).
    """
    raw = response.xpath('.//script[@id="__NEXT_DATA__"]/text()').get()
    if raw:
        try:
            next_data = json.loads(raw)
            agents = jmespath.search("props.pageProps.agents.data", next_data) or []
        except (json.JSONDecodeError, TypeError):
            agents = []
        if agents:
            out: list[Candidate] = []
            for a in agents:
                name = (a.get("name") or "").strip()
                slug = (a.get("slug") or "").strip()
                agent_id = a.get("id")
                if not name or not slug or agent_id is None:
                    continue
                # Prefer compliances[type='brn'].value (always
                # populated for licensed brokers); licenseNumber
                # is sometimes empty even when the broker is
                # licensed — see Dharam's record.
                brn = jmespath.search("compliances[?type=='brn'].value | [0]", a)
                if not brn:
                    brn = (a.get("licenseNumber") or "").strip() or None
                out.append(
                    Candidate(
                        name=name,
                        url=response.urljoin(f"/en/agent/{slug}-{agent_id}"),
                        brn=str(brn).strip() if brn else None,
                    )
                )
            return out

    # Fallback: HTML extraction (no BRN, name + url only).
    _inc(stats, "extract/search_json/fallback_used")
    out: list[Candidate] = []
    for a in response.xpath('.//a[@data-testid="agent-card-link"]'):
        url = a.xpath("./@href").get()
        name = (a.xpath("./@title").get() or "").strip()
        if not url or not name:
            continue
        out.append(Candidate(name=name, url=response.urljoin(url)))
    return out


# ----------------------------------------------------------------- agent fields


def extract_basic(
    item: PropertyFinderBrokerItem,
    agent_data: dict,
    response: Response,
    stats: StatsCollector | None = None,
) -> None:
    """Identity, specialization, experience, broker_name, BRN, superagent."""
    item.nationality = jmespath.search("nationality.name", agent_data)

    specialization = jmespath.search("position", agent_data)
    item.agent_specialization = (
        specialization.strip()
        if specialization and specialization.strip()
        else None
    )

    experience_since = jmespath.search("experienceSince", agent_data)
    item.experience_since = int(experience_since) if experience_since else None

    whatsapp_response_time = jmespath.search("avgWhatsappResponseTime", agent_data)
    # default to None when missing; reject negatives explicitly
    if whatsapp_response_time is None:
        item.whatsapp_response_time = None
    else:
        try:
            value = int(whatsapp_response_time)
            item.whatsapp_response_time = value if value >= 0 else None
        except (TypeError, ValueError):
            item.whatsapp_response_time = None

    broker_name = jmespath.search("name", agent_data)
    item.broker_name = broker_name.strip() if broker_name else None

    # BRN is shared with parse_disambiguating_profile via extract_profile_brn.
    item.brn = extract_profile_brn(response, stats)

    is_superagent = jmespath.search("superagent", agent_data)
    item.is_superagent = is_superagent if is_superagent is not None else None


def extract_listing_counts(item: PropertyFinderBrokerItem, agent_data: dict) -> None:
    """`listings_for_sale` / `_for_rent` / `_total`."""
    listings_for_sale = jmespath.search(
        "propertiesResidentialForSaleCount", agent_data
    )
    listings_for_rent = jmespath.search(
        "propertiesResidentialForRentCount", agent_data
    )
    item.listings_for_sale = (
        int(listings_for_sale) if listings_for_sale is not None else None
    )
    item.listings_for_rent = (
        int(listings_for_rent) if listings_for_rent is not None else None
    )
    item.listings_total = (item.listings_for_sale or 0) + (
        item.listings_for_rent or 0
    )


def extract_closed_transactions(
    item: PropertyFinderBrokerItem, agent_data: dict
) -> None:
    """Closed-deal counts and amounts."""
    sale = jmespath.search("claimedTransactionsSale", agent_data)
    rent = jmespath.search("claimedTransactionsRent", agent_data)
    item.closed_transaction_sale = int(sale) if sale is not None else None
    item.closed_transaction_rent = int(rent) if rent is not None else None

    # zero is a valid value: only collapse to None when both are missing
    if item.closed_transaction_sale is None and item.closed_transaction_rent is None:
        item.closed_deals_total = None
    else:
        item.closed_deals_total = (item.closed_transaction_sale or 0) + (
            item.closed_transaction_rent or 0
        )

    for src_key, dst_attr in [
        ("claimedTransactionsDealVolume", "closed_transaction_deal_value"),
        ("claimedTransactionsSaleAVGAmount", "closed_transaction_sale_avg_amount"),
        ("claimedTransactionsRentAVGAmount", "closed_transaction_rent_avg_amount"),
        ("claimedTransactionsRentTotalAmount", "closed_transaction_rent_total_amount"),
        ("claimedTransactionsSaleTotalAmount", "closed_transaction_sale_total_amount"),
    ]:
        value = jmespath.search(src_key, agent_data)
        setattr(item, dst_attr, float(value) if value is not None else None)


def extract_deal_history(item: PropertyFinderBrokerItem, agent_data: dict) -> None:
    """Recent deal dates + monthly average volume from `claimedTransactionsList`."""
    most_recent_rent = None
    most_recent_sale = None
    total_rent = 0
    total_sale = 0

    records = jmespath.search("claimedTransactionsList", agent_data) or []
    for transaction in records:
        deal_type = jmespath.search("dealType", transaction) or ""
        deal_date_str = jmespath.search("date", transaction)
        deal_date = (
            datetime.strptime(deal_date_str, "%Y-%m-%d").date()
            if deal_date_str
            else None
        )
        if "Rent" in deal_type:
            most_recent_rent = max_date(most_recent_rent, deal_date)
            total_rent += 1
        elif "Sale" in deal_type:
            most_recent_sale = max_date(most_recent_sale, deal_date)
            total_sale += 1

    item.average_monthly_deal_volume_rent = total_rent / 12
    item.average_monthly_deal_volume_sale = total_sale / 12
    item.most_recent_deal_date_rent = (
        most_recent_rent.strftime("%Y-%m-%d") if most_recent_rent else None
    )
    item.most_recent_deal_date_sale = (
        most_recent_sale.strftime("%Y-%m-%d") if most_recent_sale else None
    )


# --------------------------------------------------------------- listings stream


def aggregate_listing(listing: dict, agg: ListingAggState) -> None:
    """Update the running aggregates for one listing-API listing."""
    if (
        jmespath.search("property.is_premium", listing)
        or jmespath.search("property.is_featured", listing)
        or jmespath.search("property.is_spotlight_listing", listing)
    ):
        agg.listings_with_marketing_spend += 1

    listed_date_str = jmespath.search("property.listed_date", listing)
    listed_date = (
        datetime.strptime(listed_date_str, "%Y-%m-%dT%H:%M:%SZ")
        if listed_date_str
        else None
    )
    days_old = (
        (datetime.now(timezone.utc) - listed_date.replace(tzinfo=timezone.utc)).days
        if listed_date
        else None
    )

    property_type = jmespath.search("property.offering_type", listing)
    price = jmespath.search("property.price.value", listing)
    price = float(price) if price is not None else 0.0

    if property_type == "Residential for Rent":
        agg.total_property_rent_price += price
        agg.total_listing_age_days_rent += days_old or 0
        agg.most_recent_listing_date_rent = max_date(
            agg.most_recent_listing_date_rent, listed_date
        )
    elif property_type == "Residential for Sale":
        agg.total_property_sale_price += price
        agg.total_listing_age_days_sale += days_old or 0
        agg.most_recent_listing_date_sale = max_date(
            agg.most_recent_listing_date_sale, listed_date
        )


def finalize(item: PropertyFinderBrokerItem, agg: ListingAggState) -> dict:
    """Apply the listing-stream aggregates to the item and return its dict."""
    item.listings_with_marketing_spend = agg.listings_with_marketing_spend
    item.most_recent_listing_date_rent = (
        agg.most_recent_listing_date_rent.strftime("%Y-%m-%d")
        if agg.most_recent_listing_date_rent
        else None
    )
    item.most_recent_listing_date_sale = (
        agg.most_recent_listing_date_sale.strftime("%Y-%m-%d")
        if agg.most_recent_listing_date_sale
        else None
    )

    rent_count = item.listings_for_rent
    sale_count = item.listings_for_sale

    item.average_listing_price_rent = (
        agg.total_property_rent_price / rent_count if rent_count else None
    )
    item.average_listing_price_sale = (
        agg.total_property_sale_price / sale_count if sale_count else None
    )
    item.average_listing_age_days_rent = (
        agg.total_listing_age_days_rent / rent_count if rent_count else None
    )
    item.average_listing_age_days_sale = (
        agg.total_listing_age_days_sale / sale_count if sale_count else None
    )

    return item.to_dict()
