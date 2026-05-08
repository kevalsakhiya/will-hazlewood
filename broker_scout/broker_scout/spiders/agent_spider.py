"""PropertyFinder broker spider.

DLD-seeded fan-out: one DLD broker → search → match (BRN-first, then
exact-name, then fuzzy) → profile fetch → agency fetch → paginated
listings API → emit. The ambiguous case walks plausible candidates
profile-by-profile, comparing each one's BRN to DLD's, before giving
up and emitting an `ambiguous` stub.

JSON parsing in two layers: `__NEXT_DATA__` JSON via `jmespath` for
the primary path, HTML XPath fallbacks (with `extract/*` counters) for
defence-in-depth when PF reshapes a payload.
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
from broker_scout.spiders.base import BaseBrokerSpider


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


def _max_date(current, candidate):
    if candidate is None:
        return current
    if current is None or candidate > current:
        return candidate
    return current


def _listings_url(agent_id: str | int, page: int) -> str:
    return (
        "https://www.propertyfinder.ae/api/pwa/property/search"
        f"?sorting.sort=featured&filters.furnished=all"
        f"&pagination.limit={LISTING_PAGE_SIZE}&pagination.page={page}"
        f"&filters.utilities_price_type=notSelected"
        f"&filters.price_type=price_type_any"
        f"&filters.agent_id={agent_id}&locale=en"
    )


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
            candidates = self._extract_candidates(response)
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
            url=candidate.url,  # already absolute from _extract_candidates
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
        candidate_brn = self._extract_profile_brn(response)

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
        yield self._make_dld_stub(
            dld_broker, status="ambiguous", confidence=0.0
        )

    def _extract_profile_brn(self, response) -> str | None:
        """Pull the BRN out of an agent profile page. Tries the
        `__NEXT_DATA__` JSON first, falls back to the HTML 'Dubai
        Broker License' table cell.

        Used by both `_extract_basic` (the matched-flow path) and
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
            self.crawler.stats.inc_value("extract/brn/fallback_used")
            return fallback.strip()
        return None

    def _extract_candidates(self, response) -> list[Candidate]:
        """Pull (name, url, brn) tuples out of the search results.

        Primary source: `props.pageProps.agents.data` from the embedded
        `__NEXT_DATA__` JSON. That payload exposes each candidate's BRN
        directly (in `compliances[?type=='brn'].value`), letting us do
        an exact-BRN match in `match_candidates` without any profile
        fetch. Falls back to HTML XPath when the JSON is missing or
        malformed (defense-in-depth — Phase 7 monitor counts the
        fallback rate).
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
                    brn = jmespath.search(
                        "compliances[?type=='brn'].value | [0]", a
                    )
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
        self.crawler.stats.inc_value("extract/search_json/fallback_used")
        out: list[Candidate] = []
        for a in response.xpath('.//a[@data-testid="agent-card-link"]'):
            url = a.xpath('./@href').get()
            name = (a.xpath('./@title').get() or "").strip()
            if not url or not name:
                continue
            out.append(Candidate(name=name, url=response.urljoin(url)))
        return out

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

        self._extract_basic(item, agent_data, response)
        self._extract_listing_counts(item, agent_data)
        self._extract_closed_transactions(item, agent_data)
        self._extract_deal_history(item, agent_data)

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
                url=_listings_url(agent_id, 1),
                headers=LISTING_API_HEADERS,
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

    # ------------------------------------------------------------------ extractors

    def _extract_basic(self, item, agent_data, response):
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

        # BRN: shared with parse_disambiguating_profile via the static helper.
        item.brn = self._extract_profile_brn(response)

        is_superagent = jmespath.search("superagent", agent_data)
        item.is_superagent = is_superagent if is_superagent is not None else None

    def _extract_listing_counts(self, item, agent_data):
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

    def _extract_closed_transactions(self, item, agent_data):
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
            (
                "claimedTransactionsSaleAVGAmount",
                "closed_transaction_sale_avg_amount",
            ),
            (
                "claimedTransactionsRentAVGAmount",
                "closed_transaction_rent_avg_amount",
            ),
            (
                "claimedTransactionsRentTotalAmount",
                "closed_transaction_rent_total_amount",
            ),
            (
                "claimedTransactionsSaleTotalAmount",
                "closed_transaction_sale_total_amount",
            ),
        ]:
            value = jmespath.search(src_key, agent_data)
            setattr(item, dst_attr, float(value) if value is not None else None)

    def _extract_deal_history(self, item, agent_data):
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
                most_recent_rent = _max_date(most_recent_rent, deal_date)
                total_rent += 1
            elif "Sale" in deal_type:
                most_recent_sale = _max_date(most_recent_sale, deal_date)
                total_sale += 1

        item.average_monthly_deal_volume_rent = total_rent / 12
        item.average_monthly_deal_volume_sale = total_sale / 12
        item.most_recent_deal_date_rent = (
            most_recent_rent.strftime("%Y-%m-%d") if most_recent_rent else None
        )
        item.most_recent_deal_date_sale = (
            most_recent_sale.strftime("%Y-%m-%d") if most_recent_sale else None
        )

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
                url=_listings_url(agent_id, 1),
                headers=LISTING_API_HEADERS,
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
            yield self._finalize(item, agg)
            return

        listings = jmespath.search("listings", payload) or []
        # Page 1 with zero listings, when we expected some, signals
        # extraction trouble (deleted listings, API change, etc.).
        # Don't count later pages — last page often has fewer than 50.
        if not listings and current_page == 1 and total_page_count > 0:
            self.crawler.stats.inc_value("extract/listings_api/empty")
        for listing in listings:
            self._aggregate_listing(listing, agg)

        if total_page_count > current_page:
            yield Request(
                url=_listings_url(agent_id, current_page + 1),
                headers=LISTING_API_HEADERS,
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

        yield self._finalize(item, agg)

    def _aggregate_listing(self, listing, agg: ListingAggState):
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
            agg.most_recent_listing_date_rent = _max_date(
                agg.most_recent_listing_date_rent, listed_date
            )
        elif property_type == "Residential for Sale":
            agg.total_property_sale_price += price
            agg.total_listing_age_days_sale += days_old or 0
            agg.most_recent_listing_date_sale = _max_date(
                agg.most_recent_listing_date_sale, listed_date
            )

    def _finalize(self, item: PropertyFinderBrokerItem, agg: ListingAggState):
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
