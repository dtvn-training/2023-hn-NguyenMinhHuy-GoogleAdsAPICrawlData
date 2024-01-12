#!/bin/bash
cd /home/huynm/2023-hn-NguyenMinhHuy-GoogleAdsAPICrawlData/google_ads_api
rm /home/huynm/2023-hn-NguyenMinhHuy-GoogleAdsAPICrawlData/google_ads_api/data_crawled/resources_google_ads.csv && scrapy crawl resource_google_ads -o /home/huynm/2023-hn-NguyenMinhHuy-GoogleAdsAPICrawlData/google_ads_api/data_crawled/resources_google_ads.csv
rm /home/huynm/2023-hn-NguyenMinhHuy-GoogleAdsAPICrawlData/google_ads_api/data_crawled/resources_without_metrics.csv && scrapy crawl resource_without_metrics -o /home/huynm/2023-hn-NguyenMinhHuy-GoogleAdsAPICrawlData/google_ads_api/data_crawled/resources_without_metrics.csv
rm /home/huynm/2023-hn-NguyenMinhHuy-GoogleAdsAPICrawlData/google_ads_api/data_crawled/segments.csv && scrapy crawl segments -o /home/huynm/2023-hn-NguyenMinhHuy-GoogleAdsAPICrawlData/google_ads_api/data_crawled/segments.csv
rm /home/huynm/2023-hn-NguyenMinhHuy-GoogleAdsAPICrawlData/google_ads_api/data_crawled/metrics.csv && scrapy crawl metrics -o /home/huynm/2023-hn-NguyenMinhHuy-GoogleAdsAPICrawlData/google_ads_api/data_crawled/metrics.csv
rm /home/huynm/2023-hn-NguyenMinhHuy-GoogleAdsAPICrawlData/google_ads_api/data_crawled/attributes.csv && scrapy crawl attributes -o /home/huynm/2023-hn-NguyenMinhHuy-GoogleAdsAPICrawlData/google_ads_api/data_crawled/attributes.csv
rm /home/huynm/2023-hn-NguyenMinhHuy-GoogleAdsAPICrawlData/google_ads_api/data_crawled/attributes_without_metrics.csv && scrapy crawl attributes_without_metrics -o /home/huynm/2023-hn-NguyenMinhHuy-GoogleAdsAPICrawlData/google_ads_api/data_crawled/attributes_without_metrics.csv
