import scrapy
from google_ads_api.items import SegmentsItem

class SegmentsSpider(scrapy.Spider):
    name = "segments"
    allowed_domains = ["developers.google.com"]
    start_urls = ["https://developers.google.com/google-ads/api/fields/v15/segments"]

    def parse(self, response):
        segment_url = f"https://developers.google.com/google-ads/api/fields/v15/segments"
        yield scrapy.Request(segment_url, callback=self.parse_segments)

    def parse_segments(self, response):
        for i in range(1, 108):
            item = SegmentsItem()
            selector_segment_name = f"(//table[@class='orange responsive']/tr/th/h2[@tabindex='-1'])[{i}]/text()"
            selector_segment_field_description = f"(//table[@class='orange responsive']/tr[2]/td[2])[{i}]/text()"
            selector_segment_category = f"(//table[@class='orange responsive']/tr[3]/td[2]/code)[{i}]/text()"
            
            selector_segment_data_type = f"(//table[@class='orange responsive']/tr[4]/td[2]/code)[{i}]/text()"
            selector_segment_data_type_enum = f"(//table[@class='orange responsive']/tr[4]/td[2])[{i}]/code/section/div/text()"
            
            selector_segment_type_url = f"(//table[@class='orange responsive']/tr[5]/td[2]/code)[{i}]/text()"
            selector_segment_filterable = f"(//table[@class='orange responsive']/tr[6]/td[2])[{i}]/text()"
            selector_segment_selectable = f"(//table[@class='orange responsive']/tr[7]/td[2])[{i}]/text()"
            selector_segment_sortable = f"(//table[@class='orange responsive']/tr[8]/td[2])[{i}]/text()"
            selector_segment_repeated = f"(//table[@class='orange responsive']/tr[9]/td[2])[{i}]/text()"
            selector_segment_selectable_with = f"(//table[@class='orange responsive']/tr[10]/td[2])[{i}]/section/div/a/text()"
            item['segment_name'] = response.xpath(selector_segment_name).get()
            item['segment_field_description'] = response.xpath(selector_segment_field_description).get()
            item['segment_category'] = response.xpath(selector_segment_category).get()

            # Check if the segment_data_type is ENUM
            item['segment_data_type'] = response.xpath(selector_segment_data_type).get()
            if "\n" in item['segment_data_type']: # "\n" in item['segment_data_type'] means that the segment_data_type is ENUM, extract all the values and store them in a list, otherwise, store the value as a string 
                item['segment_data_type'] = response.xpath(selector_segment_data_type_enum).extract()

            item['segment_type_url'] = response.xpath(selector_segment_type_url).get()
            item['segment_filterable'] = response.xpath(selector_segment_filterable).get()
            item['segment_selectable'] = response.xpath(selector_segment_selectable).get()
            item['segment_sortable'] = response.xpath(selector_segment_sortable).get()
            item['segment_repeated'] = response.xpath(selector_segment_repeated).get()
            item['segment_selectable_with'] = response.xpath(selector_segment_selectable_with).extract()
            yield item
