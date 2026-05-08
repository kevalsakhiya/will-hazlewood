"""PropertyFinder broker spider — request flow and callback routing.

DLD-seeded fan-out: one DLD broker → search → match (BRN-first, then
exact-name, then fuzzy) → profile fetch → agency fetch → paginated
listings API → emit. The ambiguous case walks plausible candidates
profile-by-profile, comparing each one's BRN to DLD's, before giving
up and emitting an `ambiguous` stub.

Pure JSON-shape extractors live in [_pf_extractors.py](_pf_extractors.py).
This file keeps only callbacks, request building, and the stats-
incrementing decisions that depend on callback context.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime, timezone
from urllib.parse import quote_plus

import jmespath
from scrapy import Request

from broker_scout.common.dld_models import DLDBroker
from broker_scout.common.matching import (
    DEFAULT_FUZZY_THRESHOLD,
    Candidate,
    MatchResult,
    find_plausible_candidates,
    match_candidates,
    promote_to_brn_match,
)
from broker_scout.items import ListingAggState, PropertyFinderBrokerItem
from broker_scout.spiders import _pf_extractors as pfx
from broker_scout.spiders.base import BaseBrokerSpider


class AgentSpider(BaseBrokerSpider):
    name = "agent_spider"
    platform = "propertyfinder"
    # PF rejects bare /search?text=... with 404 unless session cookies
    # are present — fetch the agent landing page once at run start so
    # CookieMiddleware seeds the jar for every search that follows.
    warmup_url = "https://www.propertyfinder.ae/en/find-agent"
    # PF returns 404 for searches that match nothing — let those reach
    # parse_search_results so we can emit a not_found stub.
    handle_httpstatus_list = [404]

    # ------------------------------------------------------------------ DLD seeding

    def search_for_broker(self, dld_broker: DLDBroker) -> Iterable[Request]:
        name = dld_broker.broker_name_en or dld_broker.broker_name_ar
        url = (
            "https://www.propertyfinder.ae/en/find-agent/search"
            f"?text={quote_plus(name)}"
        )
        yield Request(
            url=url,
            callback=self.parse_search_results,
            cb_kwargs={"dld_broker": dld_broker},
        )

    def parse_search_results(
        self, response, dld_broker: DLDBroker
    ) -> Iterable[Request | dict]:
        # 404 = PF found nothing → empty candidate list → not_found stub.
        # Other 4xx/5xx still propagate normally; we only opt in to 404
        # via handle_httpstatus_list above.
        if response.status == 404:
            candidates: list[Candidate] = []
        else:
            candidates = pfx.extract_candidates(response, self.crawler.stats)
        threshold = self.crawler.settings.getint(
            "MATCH_FUZZY_THRESHOLD", DEFAULT_FUZZY_THRESHOLD
        )
        result = match_candidates(dld_broker, candidates, fuzzy_threshold=threshold)

        self.crawler.stats.inc_value(f"match/{result.status}")

        if result.status == "ambiguous":
            # Walk the plausibles by BRN before giving up. Each profile
            # fetch checks if its BRN equals DLD's. First match wins
            # and falls through to parse_agent; exhausted list emits
            # the ambiguous stub.
            plausibles = find_plausible_candidates(
                dld_broker, candidates, fuzzy_threshold=threshold
            )
            if plausibles:
                yield self._next_disambiguation(dld_broker, plausibles, idx=0)
                return
            # Defensive: status was ambiguous but no plausibles found.
            yield self._make_dld_stub(
                dld_broker, status="ambiguous", confidence=result.confidence
            )
            return

        if result.candidate_url is None:
            # not_found → emit DLD-only stub
            yield self._make_dld_stub(
                dld_broker, status=result.status, confidence=result.confidence
            )
            return

        # matched → fetch the candidate's profile page; thread match
        # context via cb_kwargs so the existing parse_agent → parse_agency
        # → parse_property chain can carry it forward.
        yield Request(
            url=response.urljoin(result.candidate_url),
            callback=self.parse_agent,
            cb_kwargs={"dld_broker": dld_broker, "match_result": result},
        )

    # ------------------------------------------------------------------ ambiguous BRN walk

    def _next_disambiguation(
        self,
        dld_broker: DLDBroker,
        plausibles: list[Candidate],
        idx: int,
    ) -> Request:
        """Yield a profile fetch for the next plausible candidate so
        `parse_disambiguating_profile` can compare its BRN to DLD's."""
        candidate = plausibles[idx]
        return Request(
            url=candidate.url,  # already absolute from extract_candidates
            callback=self.parse_disambiguating_profile,
            cb_kwargs={
                "dld_broker": dld_broker,
                "plausibles": plausibles,
                "idx": idx,
            },
        )

    def parse_disambiguating_profile(
        self,
        response,
        dld_broker: DLDBroker,
        plausibles: list[Candidate],
        idx: int,
    ):
        """Compare this candidate's BRN to DLD's. On match, hand the
        response to `parse_agent` (which will further promote the match
        to `exact_brn`). On no match, walk to the next plausible. On
        exhaustion, emit the ambiguous stub."""
        candidate_brn = pfx.extract_profile_brn(response, self.crawler.stats)

        if candidate_brn and candidate_brn == dld_broker.brn:
            self.crawler.stats.inc_value("match/ambiguous_disambiguated")
            # Hand off to the normal parse_agent flow with a
            # name_fuzzy intermediate; promote_to_brn_match inside
            # parse_agent will upgrade to exact_brn.
            result = MatchResult(
                status="name_fuzzy",
                confidence=0.9,
                candidate_url=plausibles[idx].url,
                candidate_brn=candidate_brn,
            )
            yield from self.parse_agent(
                response, dld_broker=dld_broker, match_result=result
            )
            return

        # No match. Try the next plausible.
        if idx + 1 < len(plausibles):
            yield self._next_disambiguation(dld_broker, plausibles, idx + 1)
            return

        # Exhausted — none of the plausibles confirmed by BRN.
        self.crawler.stats.inc_value("match/ambiguous_exhausted")
        yield self._make_dld_stub(dld_broker, status="ambiguous", confidence=0.0)

    # ------------------------------------------------------------------ parse_agent

    def parse_agent(
        self,
        response,
        dld_broker: DLDBroker,
        match_result: MatchResult,
    ):
        # Guard the __NEXT_DATA__ extraction so a missing or malformed
        # script tag doesn't crash the spider — produce stats instead
        # and let the rest of parse_agent fall through with an empty
        # agent_data (downstream extractors handle empty input cleanly).
        raw = response.xpath('.//script[@id="__NEXT_DATA__"]/text()').get()
        if not raw:
            self.crawler.stats.inc_value("extract/next_data/missing")
            next_data: dict = {}
        else:
            try:
                next_data = json.loads(raw)
            except json.JSONDecodeError:
                self.crawler.stats.inc_value("extract/next_data/bad_json")
                next_data = {}
        agent_data = jmespath.search("props.pageProps.agent", next_data) or {}
        if not agent_data:
            self.crawler.stats.inc_value("extract/agent_data/missing")

        item = PropertyFinderBrokerItem(
            agent_url=response.url,
            scrape_date=datetime.now(timezone.utc).date().isoformat(),
        )

        pfx.extract_basic(item, agent_data, response, self.crawler.stats)
        pfx.extract_listing_counts(item, agent_data)
        pfx.extract_closed_transactions(item, agent_data)
        pfx.extract_deal_history(item, agent_data)

        # Promote name match → exact_brn if PF's BRN agrees with DLD's.
        # No-op when BRNs disagree — we flag the disagreement via the
        # match/brn_drift counter so Phase 9.3.7's BRNDriftMonitor can
        # surface it without a DB query.
        if (
            item.brn
            and dld_broker.brn
            and not dld_broker.brn.startswith("NOBRN:")
            and item.brn != dld_broker.brn
        ):
            self.crawler.stats.inc_value("match/brn_drift")
        match_result = promote_to_brn_match(
            match_result, profile_brn=item.brn, dld_brn=dld_broker.brn
        )
        if match_result.status == "exact_brn":
            self.crawler.stats.inc_value("match/promoted_to_exact_brn")
        item.match_status = match_result.status
        item.match_confidence = match_result.confidence
        item.dld_brn = dld_broker.brn
        item.dld_broker_name = dld_broker.broker_name_en or dld_broker.broker_name_ar
        item.agency_name = dld_broker.office_name_en or dld_broker.office_name_ar

        agent_id = jmespath.search("id", agent_data)
        agency_slug = jmespath.search("broker.slug", agent_data)
        agency_url = (
            f"https://www.propertyfinder.ae/en/broker/{agency_slug}"
            if agency_slug
            else None
        )
        item.agency_url = agency_url

        total_page_count = jmespath.search(
            "props.pageProps.property.meta.page_count", next_data
        )
        total_page_count = int(total_page_count) if total_page_count else 0

        if agency_url:
            yield Request(
                agency_url,
                callback=self.parse_agency,
                meta={
                    "total_page_count": total_page_count,
                    "agent_id": agent_id,
                    "item": item,
                },
            )
        elif total_page_count > 0:
            yield Request(
                url=pfx.listings_url(agent_id, 1),
                headers=pfx.LISTING_API_HEADERS,
                callback=self.parse_property,
                meta={
                    "total_page_count": total_page_count,
                    "current_page": 1,
                    "item": item,
                    "agent_id": agent_id,
                    "agg": ListingAggState(),
                },
            )
        else:
            yield item.to_dict()

    # ------------------------------------------------------------------ parse_agency

    def parse_agency(self, response):
        item: PropertyFinderBrokerItem = response.meta["item"]
        total_page_count = response.meta.get("total_page_count", 0)
        agent_id = response.meta.get("agent_id")

        agency_registration_number = response.xpath(
            './/div[@data-testid="license-content"]/text()'
        ).get()
        cleaned = agency_registration_number.strip() if agency_registration_number else None
        if not cleaned:
            self.crawler.stats.inc_value("extract/agency_license/missing")
        item.agency_registration_number = cleaned

        if total_page_count > 0:
            yield Request(
                url=pfx.listings_url(agent_id, 1),
                headers=pfx.LISTING_API_HEADERS,
                callback=self.parse_property,
                meta={
                    "total_page_count": total_page_count,
                    "current_page": 1,
                    "item": item,
                    "agent_id": agent_id,
                    "agg": ListingAggState(),
                },
            )
        else:
            yield item.to_dict()

    # ------------------------------------------------------------------ parse_property

    def parse_property(self, response):
        item: PropertyFinderBrokerItem = response.meta["item"]
        current_page = response.meta["current_page"]
        total_page_count = response.meta["total_page_count"]
        agent_id = response.meta["agent_id"]
        agg: ListingAggState = response.meta["agg"]

        try:
            payload = json.loads(response.text)
        except json.JSONDecodeError:
            self.crawler.stats.inc_value("extract/listings_api/non_json")
            self.logger.warning(
                "listings API returned non-JSON for agent_id=%s page=%s; yielding partial",
                agent_id,
                current_page,
            )
            yield pfx.finalize(item, agg)
            return

        listings = jmespath.search("listings", payload) or []
        # Page 1 with zero listings, when we expected some, signals
        # extraction trouble (deleted listings, API change, etc.).
        # Don't count later pages — last page often has fewer than 50.
        if not listings and current_page == 1 and total_page_count > 0:
            self.crawler.stats.inc_value("extract/listings_api/empty")
        for listing in listings:
            pfx.aggregate_listing(listing, agg)

        if total_page_count > current_page:
            yield Request(
                url=pfx.listings_url(agent_id, current_page + 1),
                headers=pfx.LISTING_API_HEADERS,
                callback=self.parse_property,
                meta={
                    "total_page_count": total_page_count,
                    "current_page": current_page + 1,
                    "item": item,
                    "agent_id": agent_id,
                    "agg": agg,
                },
            )
            return

        yield pfx.finalize(item, agg)
