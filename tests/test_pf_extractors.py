"""Direct unit coverage for `spiders/_pf_extractors.py`.

The extractors are pure functions — these tests exercise them with
hand-crafted inputs (no Scrapy spider scaffolding). Indirect coverage
through the spider remains in `tests/test_base_spider.py`.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest
from scrapy.http import HtmlResponse, Request

from broker_scout.items import ListingAggState, PropertyFinderBrokerItem
from broker_scout.spiders import _pf_extractors as pfx


# --------------------------------------------------------------- pure helpers


def test_max_date_returns_later_value():
    a, b = date(2026, 1, 1), date(2026, 5, 1)
    assert pfx.max_date(a, b) == b
    assert pfx.max_date(b, a) == b


def test_max_date_treats_none_as_no_value():
    d = date(2026, 1, 1)
    assert pfx.max_date(None, d) == d
    assert pfx.max_date(d, None) == d
    assert pfx.max_date(None, None) is None


def test_listings_url_builds_expected_query():
    url = pfx.listings_url(12345, 3)
    assert "agent_id=12345" in url
    assert "pagination.page=3" in url
    assert f"pagination.limit={pfx.LISTING_PAGE_SIZE}" in url


# --------------------------------------------------------------- extract_basic


def _stub_response(body: str = "<html></html>", url: str = "https://x") -> HtmlResponse:
    return HtmlResponse(
        url=url, body=body.encode("utf-8"), encoding="utf-8", request=Request(url)
    )


def test_extract_basic_handles_empty_agent_data():
    item = PropertyFinderBrokerItem()
    pfx.extract_basic(item, agent_data={}, response=_stub_response(), stats=None)
    assert item.broker_name is None
    assert item.brn is None
    assert item.is_superagent is None


def test_extract_basic_populates_known_fields():
    item = PropertyFinderBrokerItem()
    agent_data = {
        "name": "Jane Doe ",
        "nationality": {"name": "British"},
        "position": " Sales ",
        "experienceSince": 2018,
        "avgWhatsappResponseTime": 42,
        "superagent": True,
    }
    pfx.extract_basic(item, agent_data, _stub_response(), stats=None)
    assert item.broker_name == "Jane Doe"
    assert item.nationality == "British"
    assert item.agent_specialization == "Sales"
    assert item.experience_since == 2018
    assert item.whatsapp_response_time == 42
    assert item.is_superagent is True


def test_extract_basic_rejects_negative_whatsapp_time():
    item = PropertyFinderBrokerItem()
    pfx.extract_basic(
        item, {"avgWhatsappResponseTime": -1}, _stub_response(), stats=None
    )
    assert item.whatsapp_response_time is None


# ----------------------------------------------------- extract_listing_counts


def test_extract_listing_counts_sums_to_total():
    item = PropertyFinderBrokerItem()
    pfx.extract_listing_counts(
        item,
        {"propertiesResidentialForSaleCount": 4, "propertiesResidentialForRentCount": 7},
    )
    assert item.listings_for_sale == 4
    assert item.listings_for_rent == 7
    assert item.listings_total == 11


def test_extract_listing_counts_missing_fields_are_zero_total():
    item = PropertyFinderBrokerItem()
    pfx.extract_listing_counts(item, {})
    assert item.listings_for_sale is None
    assert item.listings_for_rent is None
    assert item.listings_total == 0


# --------------------------------------------------- extract_closed_transactions


def test_closed_total_is_none_when_both_missing():
    item = PropertyFinderBrokerItem()
    pfx.extract_closed_transactions(item, {})
    assert item.closed_deals_total is None


def test_closed_total_treats_zero_as_real():
    item = PropertyFinderBrokerItem()
    pfx.extract_closed_transactions(
        item, {"claimedTransactionsSale": 0, "claimedTransactionsRent": 5}
    )
    assert item.closed_transaction_sale == 0
    assert item.closed_transaction_rent == 5
    assert item.closed_deals_total == 5


def test_closed_amount_fields_floated():
    item = PropertyFinderBrokerItem()
    pfx.extract_closed_transactions(
        item,
        {
            "claimedTransactionsSale": 1,
            "claimedTransactionsDealVolume": "1500000",
            "claimedTransactionsSaleAVGAmount": "750000",
        },
    )
    assert item.closed_transaction_deal_value == 1500000.0
    assert item.closed_transaction_sale_avg_amount == 750000.0


# ------------------------------------------------------- extract_deal_history


def test_deal_history_picks_latest_dates_per_type():
    item = PropertyFinderBrokerItem()
    pfx.extract_deal_history(
        item,
        {
            "claimedTransactionsList": [
                {"dealType": "Apartment Sale", "date": "2025-08-01"},
                {"dealType": "Apartment Sale", "date": "2025-12-15"},
                {"dealType": "Studio Rent", "date": "2024-06-10"},
                {"dealType": "Studio Rent", "date": "2026-04-01"},
            ]
        },
    )
    assert item.most_recent_deal_date_sale == "2025-12-15"
    assert item.most_recent_deal_date_rent == "2026-04-01"
    assert item.average_monthly_deal_volume_sale == pytest.approx(2 / 12)
    assert item.average_monthly_deal_volume_rent == pytest.approx(2 / 12)


def test_deal_history_empty_list_yields_zero_volumes():
    item = PropertyFinderBrokerItem()
    pfx.extract_deal_history(item, {})
    assert item.average_monthly_deal_volume_sale == 0
    assert item.average_monthly_deal_volume_rent == 0
    assert item.most_recent_deal_date_sale is None


# --------------------------------------------- aggregate_listing + finalize


def _listing(price=100.0, offering="Residential for Sale", listed_date=None, marketing=False):
    return {
        "property": {
            "is_premium": marketing,
            "is_featured": False,
            "is_spotlight_listing": False,
            "listed_date": listed_date,
            "offering_type": offering,
            "price": {"value": price},
        }
    }


def test_aggregate_listing_marketing_flag():
    agg = ListingAggState()
    pfx.aggregate_listing(_listing(marketing=True), agg)
    assert agg.listings_with_marketing_spend == 1


def test_aggregate_listing_routes_by_offering_type():
    agg = ListingAggState()
    pfx.aggregate_listing(_listing(price=500_000, offering="Residential for Sale"), agg)
    pfx.aggregate_listing(_listing(price=120_000, offering="Residential for Rent"), agg)
    assert agg.total_property_sale_price == 500_000
    assert agg.total_property_rent_price == 120_000


def test_finalize_returns_dict_with_averages():
    item = PropertyFinderBrokerItem(listings_for_sale=2, listings_for_rent=4)
    agg = ListingAggState(
        total_property_sale_price=1_000_000,
        total_property_rent_price=400_000,
        listings_with_marketing_spend=3,
    )
    out = pfx.finalize(item, agg)
    assert isinstance(out, dict)
    assert out["average_listing_price_sale"] == 500_000
    assert out["average_listing_price_rent"] == 100_000
    assert out["listings_with_marketing_spend"] == 3


def test_finalize_no_listings_yields_none_averages():
    item = PropertyFinderBrokerItem()
    out = pfx.finalize(item, ListingAggState())
    assert out["average_listing_price_sale"] is None
    assert out["average_listing_price_rent"] is None


# ----------------------------------------- extract_profile_brn (with stats fallback)


def test_extract_profile_brn_from_next_data():
    body = """
    <html><body><script id="__NEXT_DATA__" type="application/json">
    {"props": {"pageProps": {"agent": {"compliances": [{"value": "12345"}]}}}}
    </script></body></html>
    """
    response = _stub_response(body=body)
    assert pfx.extract_profile_brn(response, stats=None) == "12345"


def test_extract_profile_brn_html_fallback_increments_counter():
    body = """
    <html><body><table>
      <tr><td>Dubai Broker License</td><td> 99999 </td></tr>
    </table></body></html>
    """
    response = _stub_response(body=body)
    stats = MagicMock()
    assert pfx.extract_profile_brn(response, stats=stats) == "99999"
    stats.inc_value.assert_called_once_with("extract/brn/fallback_used")


def test_extract_profile_brn_returns_none_when_neither_works():
    response = _stub_response(body="<html><body>nothing</body></html>")
    stats = MagicMock()
    assert pfx.extract_profile_brn(response, stats=stats) is None
    stats.inc_value.assert_not_called()


def test_extract_profile_brn_works_without_stats():
    """Tests should be able to call the extractor without a stats arg."""
    body = """
    <html><body><table>
      <tr><td>Dubai Broker License</td><td>11111</td></tr>
    </table></body></html>
    """
    response = _stub_response(body=body)
    # No stats — fallback path increments nothing, just returns.
    assert pfx.extract_profile_brn(response) == "11111"


# ------------------------------------- extract_candidates (with stats fallback)


def test_extract_candidates_from_next_data():
    body = """
    <html><body><script id="__NEXT_DATA__" type="application/json">
    {"props": {"pageProps": {"agents": {"data": [
      {"name": "Jane", "slug": "jane", "id": 7, "compliances": [{"type": "brn", "value": "BR-1"}]},
      {"name": "John", "slug": "john", "id": 8, "compliances": []}
    ]}}}}
    </script></body></html>
    """
    response = _stub_response(body=body, url="https://www.propertyfinder.ae/en/find-agent/search?text=x")
    out = pfx.extract_candidates(response, stats=None)
    assert len(out) == 2
    assert out[0].name == "Jane"
    assert out[0].brn == "BR-1"
    assert out[0].url.endswith("/en/agent/jane-7")
    assert out[1].brn is None


def test_extract_candidates_html_fallback_increments_counter():
    body = """
    <html><body>
      <a data-testid="agent-card-link" href="/en/agent/foo-1" title="Foo"></a>
      <a data-testid="agent-card-link" href="/en/agent/bar-2" title="Bar"></a>
    </body></html>
    """
    response = _stub_response(body=body, url="https://www.propertyfinder.ae/en/find-agent/search")
    stats = MagicMock()
    out = pfx.extract_candidates(response, stats=stats)
    assert [c.name for c in out] == ["Foo", "Bar"]
    assert all(c.brn is None for c in out)
    stats.inc_value.assert_called_once_with("extract/search_json/fallback_used")
