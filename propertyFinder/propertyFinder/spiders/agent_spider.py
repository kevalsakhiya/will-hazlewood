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
        item['scrape_date'] = datetime.now().strftime('%Y-%m-%d')
        item['platform'] = 'propertyfinder'
        next_data = response.xpath('.//script[@id="__NEXT_DATA__"]/text()').get()
        next_data = json.loads(next_data)
        agent_data = jmespath.search('props.pageProps.agent', next_data)
        
        broker_name = jmespath.search('name', agent_data)
        item['broker_name'] = broker_name.strip() if broker_name else None
        
        brn = jmespath.search('licenseNumber', agent_data)
        item['brn'] = brn.strip() if brn else None
        
        listings_for_sale = jmespath.search('propertiesResidentialForSaleCount', agent_data)
        item['listings_for_sale'] = listings_for_sale.strip() if listings_for_sale else None
        
        listings_for_rent = jmespath.search('propertiesResidentialForRentCount', agent_data)
        item['listings_for_rent'] = listings_for_rent.strip() if listings_for_rent else None
        
        listings_total = listings_for_sale + listings_for_rent
        item['listings_total'] = listings_total.strip() if listings_total else None
        
        