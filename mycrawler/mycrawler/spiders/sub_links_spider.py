import scrapy
from urllib.parse import urljoin, urlparse
import pandas as pd


class SublinkSpider(scrapy.Spider):
    name = 'sub_links_spider'

    def __init__(self, seed_urls, *args, **kwargs):
        super(SublinkSpider, self).__init__(*args, **kwargs)
        self.start_urls = seed_urls
        self.failed_urls = 0
        # self.result = {}
        self.sublinks_list = []
        self.visited_urls = set()

    def parse(self, response, depth=1):
        seed_url = response.url
        sub_links = set()

        if depth > 7:
            return

        for link in response.css('a::attr(href)').getall():
            full_link = urljoin(seed_url, link)
            parsed_full_link = urlparse(full_link)
            parsed_seed_url = urlparse(seed_url)

            if parsed_full_link.netloc == parsed_seed_url.netloc:
                if full_link not in self.visited_urls:
                    self.visited_urls.add(full_link)
                    sub_links.add(full_link)

                    # Schedule a request for the sublink with depth+1
                    yield scrapy.Request(full_link, callback=self.parse, cb_kwargs={'depth': depth+1})

                # # save only 1 depth down
                # path_parts = parsed_full_link.path.strip('/').split('/')
                # if len(path_parts) == 1:
                #     sub_links.add(full_link)

        # self.result[seed_url] = list(sub_links)
        # self.sublinks_list.extend(list(sub_links))
        # if seed_url not in self.result:
        #     self.result[seed_url] = []
        # self.result[seed_url].extend(list(sub_links))
        if seed_url not in self.visited_urls:
            self.visited_urls.add(seed_url)
            sub_links.add(seed_url)
        self.sublinks_list.extend(list(sub_links))

        yield {'seed_url': seed_url, 'sub_links': list(sub_links)}

    def errback(self, failure):
        self.failed_urls += 1
        self.logger.error(repr(failure))
        self.logger.error(f'Failed URL: {failure.request.url}')

    def closed(self, reason):
        # Print the results and number of failed URLs
        print(f'Number of failed URLs: {self.failed_urls}')
        # df_results = pd.DataFrame(list(self.result.items()), columns=['seed_url', 'sub_links'])
        # df_results['sub_links'] = df_results['sub_links'].apply(lambda x: ','.join(x))
        # df_results.to_csv('crawleroutput/results_seed_subs_dict_7.csv', index=False)
        print("Results have been written to results_seed_subs_dict_7.csv")
        df_sublinks = pd.DataFrame({"sublinks": self.sublinks_list})
        df_sublinks.to_csv("crawleroutput/sublinks_7.csv", index=False)
        print("Results have been written to sublinks_7.csv")
