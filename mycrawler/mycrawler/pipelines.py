# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class MycrawlerPipeline:
    def process_item(self, item, spider):
        return item


class ResultPipeline:
    def __init__(self):
        self.results = {}

    def process_item(self, item, spider):
        seed_url = item['seed_url']
        sub_links = item['sub_links']
        self.results[seed_url] = sub_links
        return item

    def close_spider(self, spider):
        spider.result = self.results
