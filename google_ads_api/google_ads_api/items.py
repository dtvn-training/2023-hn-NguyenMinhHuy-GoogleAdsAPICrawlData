# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

# import scrapy


# class GoogleAdsApiItem(scrapy.Item):
#     # define the fields for your item here like:
#     # name = scrapy.Field()
#     pass

import scrapy

class GoogleAdsApiItem(scrapy.Item):
    resource_name = scrapy.Field()
    description =scrapy.Field()
    attributed_resource = scrapy.Field()
    # resource_fields = scrapy.Field()
    # metrics = scrapy.Field()
    # segment_name = scrapy.Field()
    # segment_field_description = scrapy.Field()
    # segment_category = scrapy.Field()
    # segment_data_type = scrapy.Field()
    # segment_type_url = scrapy.Field()
    # segment_filterable = scrapy.Field()
    # segment_selectable = scrapy.Field()
    # segment_sortable = scrapy.Field()
    # segment_repeated = scrapy.Field()
