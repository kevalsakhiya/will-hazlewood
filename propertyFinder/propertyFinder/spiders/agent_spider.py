import jmespath
from scrapy import Request, Spider
from datetime import datetime
import json
import jmespath

class AgentSpider(Spider):
    name = 'agent_spider'
    allowed_domains = ['https://www.propertyfinder.ae']
    # start_urls = ['https://www.propertyfinder.ae']
    start_urls = ['https://www.propertyfinder.ae/en/find-agent']

    def parse(self, response):
        agent_url_list = response.xpath('.//div[@data-testid="AgentList"]//a/@href').getall()
        for agent_url in agent_url_list:
            yield Request(url=response.urljoin(agent_url), callback=self.parse_agent)

    def parse_agent(self, response):
        item = {}

        next_data = response.xpath('.//script[@id="__NEXT_DATA__"]/text()').get()
        next_data = json.loads(next_data)
        agent_data = jmespath.search('props.pageProps.agent', next_data)

        item['scrape_date'] = datetime.now().strftime('%Y-%m-%d')
        item['platform'] = 'propertyfinder'
        
        broker_name = jmespath.search('name', agent_data)
        item['broker_name'] = broker_name.strip() if broker_name else None
        
        brn = jmespath.search('licenseNumber', agent_data)
        item['brn'] = brn.strip() if brn else None
        
        listings_for_sale = jmespath.search('propertiesResidentialForSaleCount', agent_data)
        item['listings_for_sale'] = listings_for_sale.strip() if listings_for_sale else None
        
        listings_for_rent = jmespath.search('propertiesResidentialForRentCount', agent_data)
        item['listings_for_rent'] = listings_for_rent.strip() if listings_for_rent else None
        
        listings_total = listings_for_sale + listings_for_rent
        item['listings_total'] = listings_total if listings_total else None

        is_superagent = jmespath.search('superagent', agent_data)
        item['is_superagent'] = is_superagent if is_superagent else None
        
        agent_id = jmespath.search('id', agent_data)

        # logic to extract data from properties
        property_data = jmespath.search('props.pageProps.property', next_data)
        property_list = jmespath.search('listings',property_data)

        # now save property data in variable to calculate after it after.
        
        most_recent_deal_date_rent = datetime.now()
        total_monthly_deal_volume_rent = 0
        most_recent_deal_date_sale = datetime.now()
        total_monthly_deal_volume_sale = 0
        unique_month_rent = set()
        unique_month_sale = set()


        
        closed_transaction_sale = jmespath.search('claimedTransactionsSale', agent_data)
        item['closed_transaction_sale'] = closed_transaction_sale.strip() if closed_transaction_sale else None

        closed_transaction_rent = jmespath.search('claimedTransactionsRent', agent_data)
        item['closed_transaction_rent'] = closed_transaction_sale.strip() if closed_transaction_sale else None

        closed_transaction_deal_volume = jmespath.search('claimedTransactionsDealVolume', agent_data)
        item['closed_transaction_deal_volume'] = closed_transaction_sale.strip() if closed_transaction_sale else None

        closed_transaction_sale_avg_amount = jmespath.search('claimedTransactionsSaleAVGAmount', agent_data)
        item['closed_transaction_sale_avg_amount'] = closed_transaction_sale.strip() if closed_transaction_sale else None

        closed_transaction_rent_avg_amount = jmespath.search('claimedTransactionsRentAVGAmount', agent_data)
        item['closed_transaction_rent_avg_amount'] = closed_transaction_sale.strip() if closed_transaction_sale else None

        closed_transaction_rent_total_amount = jmespath.search('claimedTransactionsRentTotalAmount', agent_data)
        item['closed_transaction_rent_total_amount'] = closed_transaction_sale.strip() if closed_transaction_sale else None

        closed_transaction_sale_total_amount = jmespath.search('claimedTransactionsSaleTotalAmount', agent_data)
        item['closed_transaction_sale_total_amount'] = closed_transaction_sale.strip() if closed_transaction_sale else None

        

        transaction_records_list = jmespath.search('claimedTransactionsList', agent_data)
        transaction_records_list = transaction_records_list if transaction_records_list else []
        for transaction in transaction_records_list:
            deal_type = jmespath.search('dealType', transaction)
            deal_date = jmespath.search('date', transaction)
            deal_date = datetime.strptime(deal_date, '%Y-%m-%d') if deal_date else None
            deal_price = jmespath.search('price', transaction)
            deal_price = int(deal_price) if deal_price else 0

            if "Rent" in deal_type:
                if deal_date and deal_date > most_recent_deal_date_rent:
                    most_recent_deal_date_rent = deal_date
                unique_month_rent.add(deal_date.strftime('%Y-%m'))
                total_monthly_deal_volume_rent += 1

            elif "Sale" in deal_type:
                if deal_date and deal_date > most_recent_deal_date_sale:
                    most_recent_deal_date_sale = deal_date
                unique_month_sale.add(deal_date.strftime('%Y-%m'))
                total_monthly_deal_volume_sale += 1
        
        # calculating average_listing_price_rent and average_listing_price_sale
        average_monthly_deal_volume_rent = total_monthly_deal_volume_rent / len(unique_month_rent) if unique_month_rent else 0
        average_monthly_deal_volume_sale = total_monthly_deal_volume_sale / len(unique_month_sale) if unique_month_sale else 0

        item['average_monthly_deal_volume_rent'] = average_monthly_deal_volume_rent
        item['average_monthly_deal_volume_sale'] = average_monthly_deal_volume_sale
        
        agnecy_url = jmespath.search('broker.slug', agent_data)
        agency_url = response.urljoin(agnecy_url)

        total_page_count = jmespath.search('props.pageProps.property.meta.page_count', next_data)
        total_page_count = int(total_page_count) if total_page_count else 0

        if agency_url:
            yield Request(agency_url, 
                        callback=self.parse_agency,
                        meta={
                        'total_page_count': total_page_count,
                        'item':item},
                        )
        else:
            if total_page_count > 1:
                headers = {
                'accept': '*/*',
                'accept-language': 'en-GB,en;q=0.9,en-US;q=0.8,hi;q=0.7,fr;q=0.6,gu;q=0.5',
            'locale': 'en',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36',
            }
            for page_num in range(1, total_page_count + 1):
                url = f"https://www.propertyfinder.ae/api/pwa/property/search?sorting.sort=featured&&filters.furnished=all&pagination.limit=10&pagination.page={page_num}&filters.utilities_price_type=notSelected&filters.price_type=price_type_any&filters.agent_id={agent_id}&locale=en"
                yield Request(url=url, 
                            headers=headers, 
                            callback=self.parse_property,
                            meta={
                            'total_page_count': total_page_count,
                            'current_page':page_num,
                            'item':item
                            },
                            )

    def parse_agency(self, response):
        item = response.meta.get('item')
        total_page_count = response.meta.get('total_page_count', 0)
        current_page = response.meta.get('current_page', 0)

        agency_regestration_number = response.xpath('.//div[@data-testid="license-content"]/text()').get()
        agency_regestration_number = agency_regestration_number.strip() if agency_regestration_number else None
        headers = {
                'accept': '*/*',
                'accept-language': 'en-GB,en;q=0.9,en-US;q=0.8,hi;q=0.7,fr;q=0.6,gu;q=0.5',
            'locale': 'en',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36',
            }
        for page_num in range(1, total_page_count + 1):
                url = f"https://www.propertyfinder.ae/api/pwa/property/search?sorting.sort=featured&&filters.furnished=all&pagination.limit=10&pagination.page={page_num}&filters.utilities_price_type=notSelected&filters.price_type=price_type_any&filters.agent_id={agent_id}&locale=en"
                yield Request(url=url, 
                            headers=headers, 
                            callback=self.parse_property,
                            meta={
                            'total_page_count': total_page_count,
                            'current_page':page_num,
                            'item':item
                            },
                            )

        
    def parse_property(self, response):
        item = response.meta.get('item')
        current_page = response.meta.get('current_page')
        total_page_count = response.meta.get('total_page_count')
        property_data = response.meta.get('property_data', {})

        listings_with_marketing_spend = property_data.get('listings_with_marketing_spend', 0)
        total_property_rent_price = property_data.get('total_property_rent_price', 0)
        total_property_sale_price = property_data.get('total_property_sale_price', 0)
        total_listing_age_days_rent = property_data.get('total_listing_age_days_rent', 0)
        total_listing_age_days_sale = property_data.get('total_listing_age_days_sale', 0)
        most_recent_listing_date_rent = property_data.get('most_recent_listing_date_rent', datetime.now())
        most_recent_listing_date_sale = property_data.get('most_recent_listing_date_sale', datetime.now())
        
        next_data = response.xpath('.//script[@id="__NEXT_DATA__"]/text()').get()
        next_data = json.loads(next_data)
        property_data = jmespath.search('props.pageProps.property', next_data)
        property_list = jmespath.search('listings',property_data)

        for property in property_list:
            # calculating listings_with_marketing_spend by checking if property is premium or featured or spotlight badge on it.
            is_premium = jmespath.search('property.is_premium', property)
            is_featured = jmespath.search('property.is_featured', property)
            is_spotlight_listing = jmespath.search('property.is_spotlight_listing', property)

            if is_featured or is_premium or is_spotlight_listing:
                listings_with_marketing_spend+=1
            
            # calculating average_listing_price_rent and average_listing_price_sale
            property_type = jmespath.search('property.offering_type', property)
            
            # calculating average days of the listing posted by the agent
            listed_date = jmespath.search('property.listed_date', property)

            # 2026-04-09T11:43:05Z
            listed_date = datetime.strptime(listed_date, '%Y-%m-%dT%H:%M:%SZ')if listed_date else None
            
            # calculate how many days before the listing was posted
            days_old = (datetime.now() - listed_date).days if listed_date else None

            if property_type == 'Residential for Rent':
                property_rent_price = jmespath.search('property.price.value', property)
                property_rent_price = int(property_rent_price) if property_rent_price else 0
                total_property_rent_price += property_rent_price
                total_listing_age_days_rent += days_old
                most_recent_listing_date_rent = listed_date if listed_date and listed_date > most_recent_listing_date_rent else most_recent_listing_date_rent

            elif property_type == 'Residential for Sale':
                property_sale_price = jmespath.search('property.price.value', property)
                property_sale_price = int(property_sale_price) if property_sale_price else 0
                total_property_sale_price += property_sale_price
                total_listing_age_days_sale += days_old
                most_recent_listing_date_sale = listed_date if listed_date and listed_date > most_recent_listing_date_sale else most_recent_listing_date_sale

        if total_page_count>current_page:
            headers = {
                'accept': '*/*',
                'accept-language': 'en-GB,en;q=0.9,en-US;q=0.8,hi;q=0.7,fr;q=0.6,gu;q=0.5',
            'locale': 'en',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36',
            }
            url = f"https://www.propertyfinder.ae/api/pwa/property/search?sorting.sort=featured&&filters.furnished=all&pagination.limit=10&pagination.page={current_page+1}&filters.utilities_price_type=notSelected&filters.price_type=price_type_any&filters.agent_id={agent_id}&locale=en"
            yield Request(url=url, 
                        headers=headers, 
                        callback=self.parse_property,
                        meta={
                        'total_page_count': total_page_count,
                        'current_page':current_page+1,
                        'item':item,
                        'property_data':{
                            'listings_with_marketing_spend': listings_with_marketing_spend,
                            'total_property_rent_price': total_property_rent_price,
                            'total_property_sale_price': total_property_sale_price,
                            'total_listing_age_days_rent': total_listing_age_days_rent,
                            'total_listing_age_days_sale': total_listing_age_days_sale,
                            'most_recent_listing_date_rent': most_recent_listing_date_rent,
                            'most_recent_listing_date_sale': most_recent_listing_date_sale,
                        }
                        },
                        )
        item['listings_with_marketing_spend'] = listings_with_marketing_spend
