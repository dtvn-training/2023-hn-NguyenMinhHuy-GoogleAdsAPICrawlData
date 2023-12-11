import scrapy

class GoogleAdsApiItem(scrapy.Item):
    resource_name = scrapy.Field()
    description =scrapy.Field()
    attributed_resource = scrapy.Field()
    segmenting_resource = scrapy.Field()
    with_metrics = scrapy.Field()
    list_attributes = scrapy.Field()  # List of attributes (resource fields)
    list_segments = scrapy.Field()
    list_metrics = scrapy.Field()
    

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

class AttributesItem(scrapy.Item):
    attribute_name = scrapy.Field()
    attribute_field_description = scrapy.Field()
    attribute_category = scrapy.Field()
    attribute_data_type = scrapy.Field()
    attribute_type_url = scrapy.Field()
    attribute_filterable = scrapy.Field()
    attribute_selectable = scrapy.Field()
    attribute_sortable = scrapy.Field()
    attribute_repeated = scrapy.Field()