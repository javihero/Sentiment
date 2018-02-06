# -*- coding: utf-8 -*-
import scrapy

from test_scrapy_urls.items import MyQuotes

class QuotesUrlSpider(scrapy.Spider):
    name = 'quoteurls'

    def __init__(self):
        with open('./quotes_urls.txt', 'r') as f:
            self.start_urls = [url.strip() for url in f.readlines()]
            for index, url in enumerate(self.start_urls):
                if url.endswith('/'):
                    url = url[:-1]
                    self.start_urls[index] = url
            
            print (self.start_urls)

    def parse(self, response):
        for sel in response.css('div.quote'):
            item = MyQuotes()
            item['quote'] = sel.css('span.text::text').extract()
            item['author'] = sel.css('small.author::text').extract()
            item['tags'] = sel.css('.tag::text').extract()
            yield item