import scrapy
from google_ads_api.items import MetricsItem

class MetricsSpider(scrapy.Spider):
    name = "metrics14"
    allowed_domains = ["developers.google.com"]
    start_urls = ["https://developers.google.com/google-ads/api/fields/v14/metrics"]

    def parse(self, response):
        metric_url = f"https://developers.google.com/google-ads/api/fields/v14/metrics"
        yield scrapy.Request(metric_url, callback=self.parse_metrics)

    def parse_metrics(self, response):
        for i in range(1, 157):
            item = MetricsItem()
            selector_metric_name = f"(//table[@class='green responsive']/tr/th/h2[@tabindex='-1'])[{i}]/text()"
            selector_metric_field_description = f"(//table[@class='green responsive']/tr[2]/td[2])[{i}]" # Field description often contains tags other than text, we will get all of it and then process to get only the text later
            selector_metric_category = f"(//table[@class='green responsive']/tr[3]/td[2]/code)[{i}]/text()"
            
            selector_metric_data_type = f"(//table[@class='green responsive']/tr[4]/td[2]/code)[{i}]/text()"
            selector_metric_data_type_enum = f"(//table[@class='green responsive']/tr[4]/td[2])[{i}]/code/section/div/text()"
            
            selector_metric_type_url = f"(//table[@class='green responsive']/tr[5]/td[2]/code)[{i}]/text()"
            selector_metric_filterable = f"(//table[@class='green responsive']/tr[6]/td[2])[{i}]/text()"
            selector_metric_selectable = f"(//table[@class='green responsive']/tr[7]/td[2])[{i}]/text()"
            selector_metric_sortable = f"(//table[@class='green responsive']/tr[8]/td[2])[{i}]/text()"
            selector_metric_repeated = f"(//table[@class='green responsive']/tr[9]/td[2])[{i}]/text()"
            item['metric_name'] = response.xpath(selector_metric_name).get()
            item['metric_field_description'] = response.xpath(selector_metric_field_description).get()
            item['metric_category'] = response.xpath(selector_metric_category).get()

            # Check if the segment_data_type is ENUM
            item['metric_data_type'] = response.xpath(selector_metric_data_type).get() # Get the data type
            if "\n" in item['metric_data_type']:   # "\n" in item['attribute_data_type'] means that the attribute_data_type is ENUM, extract all the values and store them in a list, otherwise, store the value as a string
                item['metric_data_type'] = response.xpath(selector_metric_data_type_enum).extract()

            item['metric_type_url'] = response.xpath(selector_metric_type_url).get()
            item['metric_filterable'] = response.xpath(selector_metric_filterable).get()
            item['metric_selectable'] = response.xpath(selector_metric_selectable).get()
            item['metric_sortable'] = response.xpath(selector_metric_sortable).get()
            item['metric_repeated'] = response.xpath(selector_metric_repeated).get()
            item['metric_selectable_with'] = "None12345"
            yield item
