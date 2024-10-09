import scrapy
from urllib.parse import urljoin, urlparse, urlunparse
import pandas as pd
from collections import defaultdict
import os


class SublinkSpider(scrapy.Spider):
    name = 'sub_links_spider'

    def __init__(self, seed_urls, *args, **kwargs):
        super(SublinkSpider, self).__init__(*args, **kwargs)
        self.start_urls = seed_urls
        self.failed_urls = 0
        # self.result = {}
        self.sublinks_list = []
        self.visited_urls = set()
        self.file_counter = 1
        self.url_counter = defaultdict(int)  # To count how many times a base URL is crawled with different params

    def parse(self, response, depth=1):
        seed_url = response.url
        sub_links = set()

        if depth > 7:
            return

        for link in response.css('a::attr(href)').getall():
            full_link = urljoin(seed_url, link)
            parsed_full_link = urlparse(full_link)

            # Remove fragment from the URL to avoid crawling the same URL with different fragments
            normalized_full_link = parsed_full_link._replace(fragment='')
            full_link_without_fragment = urlunparse(normalized_full_link)

            base_url_without_query = urlunparse(normalized_full_link._replace(query=''))

            if self.url_counter[base_url_without_query] >= 5:
                continue  # Skip this URL if it's been crawled 5 times with different params

            parsed_seed_url = urlparse(seed_url)

            if parsed_full_link.netloc == parsed_seed_url.netloc:
                if full_link_without_fragment not in self.visited_urls:
                    self.visited_urls.add(full_link_without_fragment)
                    self.url_counter[base_url_without_query] += 1
                    sub_links.add(full_link_without_fragment)

                    # Schedule a request for the sublink with depth+1
                    yield scrapy.Request(full_link_without_fragment, callback=self.parse,
                                         cb_kwargs={'depth': depth + 1})

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

        if len(self.sublinks_list) >= 100000:
            self.save_to_parquet()
            self.sublinks_list = []

        yield {'seed_url': seed_url, 'sub_links': list(sub_links)}

    def save_to_parquet(self):
        os.makedirs("crawleroutput/parquet", exist_ok=True)
        df_sublinks = pd.DataFrame({"sublinks": self.sublinks_list})
        df_sublinks.to_parquet(f"crawleroutput/parquet/sublinks_7_part_{self.file_counter}.parquet", index=False)
        print(f"Part {self.file_counter} saved.")
        self.file_counter += 1

    def errback(self, failure):
        self.failed_urls += 1
        self.logger.error(repr(failure))
        self.logger.error(f'Failed URL: {failure.request.url}')

        with open("failed_urls.txt", "a") as f:
            f.write(failure.request.url + "\n")

    def closed(self, reason):
        # Print the results and number of failed URLs
        # df_results = pd.DataFrame(list(self.result.items()), columns=['seed_url', 'sub_links'])
        # df_results['sub_links'] = df_results['sub_links'].apply(lambda x: ','.join(x))
        # df_results.to_csv('crawleroutput/results_seed_subs_dict_7.csv', index=False)
        # print("Results have been written to results_seed_subs_dict_7.csv")
        # df_sublinks = pd.DataFrame({"sublinks": self.sublinks_list})
        # df_sublinks.to_csv("crawleroutput/sublinks_7.csv", index=False)
        # print("Results have been written to sublinks_7.csv")
        if self.sublinks_list:
            self.save_to_parquet()
        print(f'Number of failed URLs: {self.failed_urls}')
