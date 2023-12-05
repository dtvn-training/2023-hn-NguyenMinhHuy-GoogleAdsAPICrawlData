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
    
    # def parse(self, response):
    #     # loop from 1 to 107
    #     for i in range(1, 108):

    #     yield scrapy.Request(response.urljoin(f"https://developers.google.com/google-ads/api/fields/v15/segments#{i}"), callback=self.parse_resource)
    # write parse function to loop from 1 to 107 and yield scrapy.Request to each page
    def parse(self, response):
        for i in range(1, 108):
            segment_url = f"https://developers.google.com/google-ads/api/fields/v15/segments#{i}"
            yield scrapy.Request(segment_url, callback=self.parse_segments, meta={'index': i})
            # yield scrapy.Request("https://developers.google.com/google-ads/api/fields/v15/segments", callback=self.parse_segments, meta={'index': i})             

    # why this code does not loop through all the segments?
    # answer: because the loop is in the parse function, not in the parse_segments function
    # def parse(self, response):
    #     # loop from 1 to 107
    #     for i in range(1, 108):
    #         yield scrapy.Request(response.urljoin(f"https://developers.google.com/google-ads/api/fields/v15/segments#{i}"), callback=self.parse_segments, meta={'index': i})
    

    def parse_segments(self, response):
        item = SegmentsItem()
        i = response.meta.get('index', None)
        # Create a selector for each element
        selector_segment_name = f"//h2[@tabindex='-1'][{i}]/text()"
        selector_segment_field_description = f"//table[@class='orange responsive'][{i}]/tr[2]/td[2]/text()"
        selector_segment_category = f"//table[@class='orange responsive'][{i}]/tr[3]/td[2]/code/text()"
        selector_segment_data_type = f"//table[@class='orange responsive'][{i}]/tr[4]/td[2]/code/text()"
        selector_segment_type_url = f"//table[@class='orange responsive'][{i}]/tr[5]/td[2]/code/text()"
        selector_segment_filterable = f"//table[@class='orange responsive'][{i}]/tr[6]/td[2]/text()"
        selector_segment_selectable = f"//table[@class='orange responsive'][{i}]/tr[7]/td[2]/text()"
        selector_segment_sortable = f"//table[@class='orange responsive'][{i}]/tr[8]/td[2]/text()"
        selector_segment_repeated = f"//table[@class='orange responsive'][{i}]/tr[9]/td[2]/text()"
        item['segment_name'] = response.xpath(selector_segment_name).get()
        item['segment_field_description'] = response.xpath(selector_segment_field_description).get()
        item['segment_category'] = response.xpath(selector_segment_category).get()
        item['segment_data_type'] = response.xpath(selector_segment_data_type).get()
        item['segment_type_url'] = response.xpath(selector_segment_type_url).get()
        item['segment_filterable'] = response.xpath(selector_segment_filterable).get()
        item['segment_selectable'] = response.xpath(selector_segment_selectable).get()
        item['segment_sortable'] = response.xpath(selector_segment_sortable).get()
        item['segment_repeated'] = response.xpath(selector_segment_repeated).get()
        yield item

