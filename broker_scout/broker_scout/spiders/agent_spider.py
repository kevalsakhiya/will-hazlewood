import json
from datetime import datetime, timezone
from urllib.parse import quote_plus

import jmespath
from scrapy import Request, Spider

from broker_scout.items import ListingAggState, PropertyFinderBrokerItem


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


class AgentSpider(Spider):
    name = "agent_spider"
    start_urls = ["https://www.propertyfinder.ae/en/find-agent"]

    def parse(self, response):
        agent_name = "DHARAM VIR JUNEJA"
        url = (
            "https://www.propertyfinder.ae/en/find-agent/search"
            f"?text={quote_plus(agent_name)}"
        )
        yield Request(url=url, callback=self.parse_search_results)

    def parse_search_results(self, response):
        agent_urls = response.xpath(
            './/div[@data-testid="AgentList"]//li//a/@href'
        ).getall()
        for agent_url in agent_urls:
            yield Request(url=response.urljoin(agent_url), callback=self.parse_agent)

    # ------------------------------------------------------------------ parse_agent

    def parse_agent(self, response):
        next_data = json.loads(
            response.xpath('.//script[@id="__NEXT_DATA__"]/text()').get()
        )
        agent_data = jmespath.search("props.pageProps.agent", next_data) or {}

        item = PropertyFinderBrokerItem(
            agent_url=response.url,
            scrape_date=datetime.now(timezone.utc).date().isoformat(),
        )

        self._extract_basic(item, agent_data, response)
        self._extract_listing_counts(item, agent_data)
        self._extract_closed_transactions(item, agent_data)
        self._extract_deal_history(item, agent_data)

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

        # BRN: keep as string, default None when missing
        brn = jmespath.search("compliances[-1].value", agent_data)
        brn = str(brn).strip() if brn is not None else None
        if not brn:
            fallback = response.xpath(
                './/td[contains(text(),"Dubai Broker License")]'
                "/following-sibling::td/text()"
            ).get()
            brn = fallback.strip() if fallback and fallback.strip() else None
            if brn:
                self.logger.warning(
                    "brn missing in __NEXT_DATA__, using HTML fallback for %s",
                    response.url,
                )
        item.brn = brn or None

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
        item.agency_registration_number = (
            agency_registration_number.strip() if agency_registration_number else None
        )

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
            self.logger.warning(
                "listings API returned non-JSON for agent_id=%s page=%s; yielding partial",
                agent_id,
                current_page,
            )
            yield self._finalize(item, agg)
            return

        for listing in jmespath.search("listings", payload) or []:
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
