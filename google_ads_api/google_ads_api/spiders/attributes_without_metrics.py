import scrapy
from google_ads_api.items import AttributesItem

class ResourceWithoutMetricsSpider(scrapy.Spider):
    name = "attributes_without_metrics"
    allowed_domains = ["developers.google.com"]
    start_urls = ["https://developers.google.com/google-ads/api/fields/v15/overview"]

    def parse(self, response):
    # Loop through each element from nth-child(3) to nth-child(60)    
        for i in range(1, 114):
        # Create a selector for each element
            
            selector = f"body > section > devsite-book-nav > nav > div.devsite-book-nav-wrapper > div.devsite-mobile-nav-bottom > ul > li > div > ul > li:nth-child(1) > div > ul > li:nth-child(4) > div > ul > li:nth-child({i}) > a"
        # Extract the text from the element
            item_url = response.css(selector + "::attr(href)").extract_first()
        # Check if the URL starts with 'javascript:'
            if item_url and item_url.startswith('javascript:'):
                continue  # Skip this URL
        # If it's a valid URL, create a Request object
            yield scrapy.Request(response.urljoin(item_url), callback=self.parse_attribute)
        
        # item_url = response.css('body > section > devsite-book-nav > nav > div.devsite-book-nav-wrapper > div.devsite-mobile-nav-bottom > ul > li > div > ul > li:nth-child(1) > div > ul > li:nth-child(2) > a > span')

        # If there's a next page, click it and repeat the process
        next_page = response.css("li.next > a ::attr(href)").extract_first()
        if next_page:
            yield scrapy.Request(response.urljoin(next_page), callback=self.parse)
    
    def parse_attribute(self, response):
        i = 1
        while True:
            item = AttributesItem()
            selector = f"//table[@class='blue responsive']/tr/th/h2[@tabindex='-1'][{i}]/text()"
            if response.xpath(selector).get() == "":
                break
            else:
                selector_attribute_name = f"(//table[@class='blue responsive']/tr/th/h2[@tabindex='-1'])[{i}]/text()"
                selector_attribute_field_description = f"(//table[@class='blue responsive']/tr[2]/td[2])[{i}]" # Field description often contains tags other than text, we will get all of it and then process to get only the text later
                selector_attribute_category = f"(//table[@class='blue responsive']/tr[3]/td[2]/code)[{i}]/text()"

                selector_attribute_data_type = f"(//table[@class='blue responsive']/tr[4]/td[2]/code)[{i}]/text()"
                selector_attribute_data_type_enum = f"(//table[@class='blue responsive']/tr[4]/td[2])[{i}]/code/section/div/text()"
                
                selector_attribute_type_url = f"(//table[@class='blue responsive']/tr[5]/td[2]/code)[{i}]/text()"
                selector_attribute_filterable = f"(//table[@class='blue responsive']/tr[6]/td[2])[{i}]/text()"
                selector_attribute_selectable = f"(//table[@class='blue responsive']/tr[7]/td[2])[{i}]/text()"
                selector_attribute_sortable = f"(//table[@class='blue responsive']/tr[8]/td[2])[{i}]/text()"
                selector_attribute_repeated = f"(//table[@class='blue responsive']/tr[9]/td[2])[{i}]/text()"
                item['attribute_name'] = response.xpath(selector_attribute_name).get()
                item['attribute_field_description'] = response.xpath(selector_attribute_field_description).get()
                item['attribute_category'] = response.xpath(selector_attribute_category).get()

                item['attribute_data_type'] = response.xpath(selector_attribute_data_type).get()
                if "\n" in response.xpath(selector_attribute_data_type).get(): 
                    item['attribute_data_type'] = response.xpath(selector_attribute_data_type_enum).extract()
                else:
                    item['attribute_data_type'] = response.xpath(selector_attribute_data_type).get()

                item['attribute_type_url'] = response.xpath(selector_attribute_type_url).get()
                item['attribute_filterable'] = response.xpath(selector_attribute_filterable).get()
                item['attribute_selectable'] = response.xpath(selector_attribute_selectable).get()
                item['attribute_sortable'] = response.xpath(selector_attribute_sortable).get()
                item['attribute_repeated'] = response.xpath(selector_attribute_repeated).get()
                item['attribute_selectable_with'] = None
                yield item
                i += 1
