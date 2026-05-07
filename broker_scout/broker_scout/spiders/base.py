"""`BaseBrokerSpider` — abstract platform-agnostic spider.

Drives `start_requests` from the DLD registry: one DLD broker → one
search request → one matched/ambiguous/not_found item, **always**.
Subclasses (PropertyFinder, Bayut later) implement the platform-
specific search URL and candidate extraction; the rest of the
lifecycle (DLD seeding, stub emission, threading match metadata) is
handled here.
"""

from __future__ import annotations

import logging
from abc import abstractmethod
from datetime import UTC, datetime
from itertools import islice
from typing import Iterable

from scrapy import Request, Spider

from broker_scout.common import dld_repo
from broker_scout.common.dld_models import DLDBroker
from broker_scout.items import PropertyFinderBrokerItem

logger = logging.getLogger(__name__)


class BaseBrokerSpider(Spider):
    """Common scaffolding for DLD-seeded broker spiders.

    Subclasses must set:
        * `name`     — Scrapy spider identifier.
        * `platform` — `"propertyfinder"` | `"bayut"`.
        * `search_for_broker(dld_broker)` → request(s) for the search
                                            endpoint.
        * `parse_search_results(response, dld_broker)` → match → yield
                                                         match request
                                                         OR stub item.
    """

    platform: str = ""
    # If set, BaseBrokerSpider issues a single GET to this URL first,
    # then dispatches DLD searches from its callback. Lets the platform
    # set session cookies (PF rejects bare /search?text=... with 404
    # when no warmup cookies are present). Subclasses opt in.
    warmup_url: str | None = None

    # ----------------------------------------------------- start_requests

    def start_requests(self) -> Iterable[Request | dict]:
        """Issue an optional warmup GET, then fan out one search per DLD
        broker (with a `not_found` short-circuit for nameless rows)."""
        if self.warmup_url:
            yield Request(
                self.warmup_url,
                callback=self._dispatch_dld_searches,
                dont_filter=True,
            )
        else:
            yield from self._dispatch_dld_searches(response=None)

    def _dispatch_dld_searches(self, response):
        limit = self.settings.getint("DLD_LIMIT", 0) or None
        brn_filter_str = self.settings.get("DLD_BRN_FILTER", "") or ""
        allowed_brns = {b.strip() for b in brn_filter_str.split(",") if b.strip()}

        brokers = dld_repo.iter_active_brokers()
        if allowed_brns:
            brokers = (b for b in brokers if b.brn in allowed_brns)
        if limit:
            brokers = islice(brokers, limit)

        count = 0
        for dld_broker in brokers:
            count += 1
            if not (dld_broker.broker_name_en or dld_broker.broker_name_ar):
                yield self._make_dld_stub(dld_broker, status="not_found")
                continue
            yield from self.search_for_broker(dld_broker)

        self.logger.info(
            "dispatched %d DLD brokers (limit=%s)", count, limit
        )

    # ----------------------------------------------------- abstract hooks

    @abstractmethod
    def search_for_broker(
        self, dld_broker: DLDBroker
    ) -> Iterable[Request | dict]:
        """Yield search request(s) for one DLD broker. Subclass-specific."""
        raise NotImplementedError

    @abstractmethod
    def parse_search_results(self, response, dld_broker: DLDBroker):
        """Extract candidates, run matching, yield either:
          * a profile-fetch request for the picked candidate, OR
          * a DLD-only stub item for ambiguous / not_found cases.
        """
        raise NotImplementedError

    # ----------------------------------------------------- helpers

    def _make_dld_stub(
        self,
        dld_broker: DLDBroker,
        status: str,
        confidence: float | None = None,
    ) -> dict:
        """Build a DLD-only stub item that flows through validation and
        all downstream sinks, so coverage is auditable per-platform."""
        scrape_date = getattr(self, "scrape_date", None) or datetime.now(UTC).date().isoformat()
        item = PropertyFinderBrokerItem(
            platform=self.platform or "propertyfinder",
            scrape_date=scrape_date,
            match_status=status,
            match_confidence=confidence,
            dld_brn=dld_broker.brn,
            dld_broker_name=dld_broker.broker_name_en or dld_broker.broker_name_ar,
            agency_name=dld_broker.office_name_en or dld_broker.office_name_ar,
        )
        return item.to_dict()
