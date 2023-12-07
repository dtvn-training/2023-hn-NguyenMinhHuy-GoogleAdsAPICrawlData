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

class SegmentsItem(scrapy.Item):
    segment_name = scrapy.Field()
    segment_field_description = scrapy.Field()
    segment_category = scrapy.Field()
    segment_data_type = scrapy.Field()
    segment_type_url = scrapy.Field()
    segment_filterable = scrapy.Field()
    segment_selectable = scrapy.Field()
    segment_sortable = scrapy.Field()
    segment_repeated = scrapy.Field()
    
class MetricsItem(scrapy.Item):
    metric_name = scrapy.Field()
    metric_field_description = scrapy.Field()
    metric_category = scrapy.Field()
    metric_data_type = scrapy.Field()
    metric_type_url = scrapy.Field()
    metric_filterable = scrapy.Field()
    metric_selectable = scrapy.Field()
    metric_sortable = scrapy.Field()
    metric_repeated = scrapy.Field()