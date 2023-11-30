import scrapy
from google_ads_api.items import GoogleAdsApiItem
from w3lib.html import remove_tags

class ResourceGoogleAdsSpider(scrapy.Spider):
    name = "resource_google_ads"
    allowed_domains = ["developers.google.com"]
    start_urls = ["https://developers.google.com/google-ads/api/fields/v15/overview"]

    def parse(self, response):
    # Lặp qua các phần tử từ nth-child(3) đến nth-child(60)
        
        for i in range(3, 61):
        # Tạo selector cho mỗi phần tử
            selector = f"body > section > devsite-book-nav > nav > div.devsite-book-nav-wrapper > div.devsite-mobile-nav-bottom > ul > li > div > ul > li:nth-child(1) > div > ul > li:nth-child(3) > div > ul > li:nth-child({i}) > a"
          
        # Lấy giá trị href từ phần tử đó
            item_url = response.css(selector + "::attr(href)").extract_first()

        # Check if the URL starts with 'javascript:'
            if item_url and item_url.startswith('javascript:'):
                continue  # Skip this URL

        # If it's a valid URL, create a Request object
            yield scrapy.Request(response.urljoin(item_url), callback=self.parse_resource, meta={'index': i})
        
        item_url = response.css('body > section > devsite-book-nav > nav > div.devsite-book-nav-wrapper > div.devsite-mobile-nav-bottom > ul > li > div > ul > li:nth-child(1) > div > ul > li:nth-child(2) > a > span')


        # nếu có sản phẩm kế tiếp thì tiếp tục crawl
        next_page = response.css("li.next > a ::attr(href)").extract_first()
        if next_page:
            yield scrapy.Request(response.urljoin(next_page), callback=self.parse)
    
    def parse_resource(self, response):
        item = GoogleAdsApiItem()
        
        i = response.meta.get('index', None)
        # Tạo selector cho mỗi phần tử
        selector2 = f"body > section > devsite-book-nav > nav > div.devsite-book-nav-wrapper > div.devsite-mobile-nav-bottom > ul > li > div > ul > li:nth-child(1) > div > ul > li:nth-child(3) > div > ul > li:nth-child({i}) > a > span"
        item['resource_name'] = response.css(selector2 + "::text").extract_first()
        item['description'] = response.css('.devsite-article-body > p:nth-child(2)::text').extract_first()

        # item['attributed_resource'] = response.xpath("//tbody[@class='list']/tr/td/a/text()").extract()
        # i want to specify the first tbody that has class = list because there are many tbody that has class = list
        # if the th tag has values "Attributed Resource" then get the text of a tag
        if response.xpath("//table[@class='columns blue responsive']/thead/tr/th/text()").extract_first() == "Attributed resources":
            item['attributed_resource'] = response.xpath("(//tbody[@class='list'])[1]/tr/td/a/text()").extract()  
        else:
            item['attributed_resource'] = "None"
        # item['attributed_resource'] = response.xpath("(//tbody[@class='list'])[1]/tr/td/a/text()").extract()    
        # item['attributed_resource'] = response.xpath('//*[@id="gc-wrapper"]/main/devsite-content/article/div[2]/devsite-filter[1]/div/table/tbody/tr[1]/td/a/text()').extract()
        yield item
# In [5]: response.xpath("//tbody[@class='list']/tr/td/a/text()").get()
# Out[5]: 'accessible_bidding_strategy'

# scrapy shell https://developers.google.com/google-ads/api/fields/v15/ad_group 