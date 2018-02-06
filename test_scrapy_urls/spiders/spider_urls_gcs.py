# -*- coding: utf-8 -*-
import scrapy

from test_scrapy_urls.items import MyQuotes

# Imports the Google Cloud client library
from google.cloud import storage


class QuotesUrlSpider(scrapy.Spider):
    name = 'urlsgcs'
    bucket_name = 'ronalbucket'
    file_name = 'quotes_urls.txt'

    def __init__(self):
        # Instantiates a client
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.bucket_name)
        blob = bucket.get_blob(self.file_name)
        file = blob.download_as_string()

        urls = file.splitlines()
        self.start_urls = urls

        for index, url in enumerate(self.start_urls):
            url = url.decode('utf-8')
            if url.endswith('/'):
                url = url[:-1]
                self.start_urls[index] = url

    def parse(self, response):
        for sel in response.css('div.quote'):
            item = MyQuotes()
            item['quote'] = sel.css('span.text::text').extract()
            item['author'] = sel.css('small.author::text').extract()
            item['tags'] = sel.css('.tag::text').extract()
            yield item
