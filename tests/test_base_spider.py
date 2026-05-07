"""Coverage for `spiders.base.BaseBrokerSpider` — DLD seeding,
DLD_LIMIT, no-name short-circuit, and stub schema validity."""

from __future__ import annotations

from dataclasses import asdict
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from broker_scout.common.dld_models import DLDBroker
from broker_scout.schemas import PropertyFinderBrokerSchema
from broker_scout.spiders.base import BaseBrokerSpider


class _ConcreteSpider(BaseBrokerSpider):
    """Minimal subclass so we can instantiate the abstract base in tests."""

    name = "test_spider"
    platform = "propertyfinder"

    def search_for_broker(self, dld_broker):
        # Single sentinel request per broker so the test can count them.
        yield {"_search_for": dld_broker.brn}

    def parse_search_results(self, response, dld_broker):  # pragma: no cover
        raise NotImplementedError


def _dld(name="DHARAM VIR JUNEJA", brn="81462") -> DLDBroker:
    return DLDBroker(
        brn=brn,
        office_license_number=None,
        broker_name_en=name,
        broker_name_ar=None,
        phone=None, mobile=None, email=None,
        real_estate_number=None,
        office_name_en="Test Office",
        office_name_ar=None,
        card_issue_date=date(2020, 1, 1), card_expiry_date=None,
        office_issue_date=None, office_expiry_date=None,
        photo_url=None, office_logo_url=None,
        card_rank_id=None, card_rank=None,
        office_rank_id=None, office_rank=None,
        awards_count=None,
    )


def _spider(dld_limit: int = 0) -> _ConcreteSpider:
    """Build a spider instance with a settings-like object the
    BaseBrokerSpider.start_requests can call .getint(...) on."""
    spider = _ConcreteSpider()
    settings = MagicMock()
    settings.getint.return_value = dld_limit
    # scrapy stores settings on self.settings via crawler; mirror that.
    spider.settings = settings
    return spider


# ============================================================ start_requests


def test_start_requests_yields_one_dispatch_per_broker():
    spider = _spider()
    brokers = [_dld(brn=str(i)) for i in range(3)]
    with patch(
        "broker_scout.spiders.base.dld_repo.iter_active_brokers",
        return_value=iter(brokers),
    ):
        # No warmup_url on _ConcreteSpider → goes straight to dispatch.
        out = list(spider.start_requests())
    # Each broker → one sentinel dict from search_for_broker
    assert len(out) == 3
    assert [r["_search_for"] for r in out] == ["0", "1", "2"]


def test_start_requests_with_warmup_yields_request_then_dispatches():
    """When warmup_url is set, start_requests yields a single warmup
    Request whose callback fans out the DLD searches. The callback is
    `_dispatch_dld_searches`."""
    from scrapy import Request as _Request

    spider = _spider()
    spider.warmup_url = "https://example.com/warmup"
    with patch(
        "broker_scout.spiders.base.dld_repo.iter_active_brokers",
        return_value=iter([_dld(brn="X")]),
    ):
        out = list(spider.start_requests())
    assert len(out) == 1
    req = out[0]
    assert isinstance(req, _Request)
    assert req.url == "https://example.com/warmup"
    assert req.callback == spider._dispatch_dld_searches

    # And the callback fans out the searches.
    with patch(
        "broker_scout.spiders.base.dld_repo.iter_active_brokers",
        return_value=iter([_dld(brn="A"), _dld(brn="B")]),
    ):
        dispatched = list(spider._dispatch_dld_searches(response=None))
    assert [r["_search_for"] for r in dispatched] == ["A", "B"]


def test_start_requests_short_circuits_when_no_name():
    """A DLD broker with no name (rare) should yield a not_found stub
    instead of a search request — we have no key to query PF with."""
    spider = _spider()
    nameless = DLDBroker(
        brn="x",
        office_license_number=None,
        broker_name_en=None, broker_name_ar=None,
        phone=None, mobile=None, email=None,
        real_estate_number=None,
        office_name_en=None, office_name_ar=None,
        card_issue_date=None, card_expiry_date=None,
        office_issue_date=None, office_expiry_date=None,
        photo_url=None, office_logo_url=None,
        card_rank_id=None, card_rank=None,
        office_rank_id=None, office_rank=None,
        awards_count=None,
    )
    with patch(
        "broker_scout.spiders.base.dld_repo.iter_active_brokers",
        return_value=iter([nameless]),
    ):
        out = list(spider.start_requests())
    assert len(out) == 1
    item = out[0]
    assert isinstance(item, dict)
    assert item["match_status"] == "not_found"
    assert item["dld_brn"] == "x"


def test_start_requests_respects_dld_limit():
    spider = _spider(dld_limit=2)
    brokers = [_dld(brn=str(i)) for i in range(10)]
    with patch(
        "broker_scout.spiders.base.dld_repo.iter_active_brokers",
        return_value=iter(brokers),
    ):
        out = list(spider.start_requests())
    assert len(out) == 2


def test_start_requests_zero_limit_means_no_cap():
    spider = _spider(dld_limit=0)
    brokers = [_dld(brn=str(i)) for i in range(7)]
    with patch(
        "broker_scout.spiders.base.dld_repo.iter_active_brokers",
        return_value=iter(brokers),
    ):
        out = list(spider.start_requests())
    assert len(out) == 7


# ============================================================ _make_dld_stub


def test_stub_passes_validation_schema():
    """The stub item dict must validate cleanly through the same
    pydantic schema all other items go through — otherwise it'll get
    dropped by ValidationPipeline."""
    spider = _spider()
    spider.scrape_date = date.today().isoformat()
    stub = spider._make_dld_stub(_dld(), status="not_found")
    PropertyFinderBrokerSchema.model_validate(stub)


def test_stub_carries_dld_ground_truth():
    spider = _spider()
    spider.scrape_date = date.today().isoformat()
    stub = spider._make_dld_stub(_dld(name="Foo Bar", brn="81462"), status="ambiguous")
    assert stub["match_status"] == "ambiguous"
    assert stub["dld_brn"] == "81462"
    assert stub["dld_broker_name"] == "Foo Bar"
    assert stub["agency_name"] == "Test Office"
    # Stubs should NOT carry PF-side fields (no profile was ever fetched)
    assert stub["agent_url"] is None
    assert stub["broker_name"] is None
    assert stub["brn"] is None


def test_stub_falls_back_to_arabic_name_when_english_missing():
    spider = _spider()
    spider.scrape_date = date.today().isoformat()
    ar_only = _dld(name=None)
    # mutate via dataclasses.replace since DLDBroker is frozen
    from dataclasses import replace

    ar_only = replace(ar_only, broker_name_en=None, broker_name_ar="عربي")
    stub = spider._make_dld_stub(ar_only, status="not_found")
    assert stub["dld_broker_name"] == "عربي"


def test_stub_default_platform_when_subclass_omits():
    """A pathological subclass forgetting to set `platform` should still
    produce a valid item (defaulted to 'propertyfinder')."""

    class NoPlatformSpider(BaseBrokerSpider):
        name = "no_platform"
        # platform deliberately not set (inherits "" from base)

        def search_for_broker(self, dld_broker):  # pragma: no cover
            return iter([])

        def parse_search_results(self, response, dld_broker):  # pragma: no cover
            return None

    s = NoPlatformSpider()
    s.scrape_date = date.today().isoformat()
    stub = s._make_dld_stub(_dld(), status="not_found")
    assert stub["platform"] == "propertyfinder"
    PropertyFinderBrokerSchema.model_validate(stub)
