import scrapy
# from google_ads_api.items import GoogleAdsApiItem
from google_ads_api.items import SegmentsItem

class SegmentsSpider(scrapy.Spider):
    name = "segments"
    allowed_domains = ["developers.google.com"]
    start_urls = ["https://developers.google.com/google-ads/api/fields/v15/segments"]

    # def parse(self, response):
    # # Loop through each element from nth-child(3) to nth-child(60)    
    #     for i in range(3, 61):
    #     # Create a selector for each element
    #         selector = f"body > section > devsite-book-nav > nav > div.devsite-book-nav-wrapper > div.devsite-mobile-nav-bottom > ul > li > div > ul > li:nth-child(1) > div > ul > li:nth-child(3) > div > ul > li:nth-child({i}) > a"
    #     # Extract the text from the element
    #         item_url = response.css(selector + "::attr(href)").extract_first()
    #     # Check if the URL starts with 'javascript:'
    #         if item_url and item_url.startswith('javascript:'):
    #             continue  # Skip this URL
    #     # If it's a valid URL, create a Request object
    #         yield scrapy.Request(response.urljoin(item_url), callback=self.parse_resource, meta={'index': i})
        
    #     item_url = response.css('body > section > devsite-book-nav > nav > div.devsite-book-nav-wrapper > div.devsite-mobile-nav-bottom > ul > li > div > ul > li:nth-child(1) > div > ul > li:nth-child(2) > a > span')

    #     # If there's a next page, click it and repeat the process
    #     next_page = response.css("li.next > a ::attr(href)").extract_first()
    #     if next_page:
    #         yield scrapy.Request(response.urljoin(next_page), callback=self.parse)
    
    def parse_segments(self, response):
        item = SegmentsItem()
        item['segment_name'] = response.css('.devsite-article-body > h1::text').extract_first()
        item['segment_field_description'] = response.css('.devsite-article-body > p:nth-child(2)::text').extract_first()
        item['segment_category'] = response.css('.devsite-article-body > p:nth-child(3)::text').extract_first()
        item['segment_data_type'] = response.css('.devsite-article-body > p:nth-child(4)::text').extract_first()
        item['segment_type_url'] = response.css('.devsite-article-body > p:nth-child(5) > a::attr(href)').extract_first()
        item['segment_filterable'] = response.css('.devsite-article-body > p:nth-child(6)::text').extract_first()
        item['segment_selectable'] = response.css('.devsite-article-body > p:nth-child(7)::text').extract_first()
        item['segment_sortable'] = response.css('.devsite-article-body > p:nth-child(8)::text').extract_first()
        item['segment_repeated'] = response.css('.devsite-article-body > p:nth-child(9)::text').extract_first()

        yield item


# response.xpath("//div[2]/devsite-filter/div[@class='devsite-table-wrapper']/table/tbody[@class='list']/tr[1]/td/div/table/tbody/tr[2]/td[2]/text()").get()
# https://developers.google.com/google-ads/api/fields/v15/segments
response.css("div:nth-child(2) devsite-filter div.devsite-table-wrapper table tbody.list tr:nth-child(1) td div table tbody tr:nth-child(2) td:nth-child(2)::attr(text)").get()
response.css("div:nth-child(2) > devsite-filter > div.devsite-table-wrapper > table >  tbody.list > tr:nth-child(1) > td > div > table > tbody > tr:nth-child(2) > td:nth-child(2)::attr(text)").get()