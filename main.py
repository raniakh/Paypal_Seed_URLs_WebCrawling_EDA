import pandas as pd
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from mycrawler.mycrawler.spiders.sub_links_spider import SublinkSpider


if __name__ == '__main__':
    df = pd.read_csv('data/datahack_sample.csv')
    urls = df.drop_duplicates().copy()
    # urls = df.head(30) # for testing
    urls['seed_url'] = urls['seed_url'].apply(lambda x: x if x.startswith('http') else 'http://' + x)
    seed_urls = urls['seed_url'].tolist()

    process = CrawlerProcess(settings=get_project_settings())
    process.crawl(SublinkSpider, seed_urls=seed_urls)
    process.start()
