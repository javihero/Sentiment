# -*- coding: utf-8 -*-
import scrapy

from test_scrapy_urls.items import MyQuotes
import os
from subprocess import call


# Imports the Google Cloud client library
from google.cloud import storage

#Import the Google Cloud BigQuery library
from google.cloud import bigquery
from google.cloud.bigquery import Dataset


class QuotesUrlSpider(scrapy.Spider):
    name = 'urlsgcs'
    bucket_name = 'ronalbucket'
    file_name = 'quotes_urls.txt'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service-account-file.json"
    
    def __init__(self):
        
        try:
            os.remove('result.jl')
        except OSError:
            pass

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

    
    def createBigQuery():
        
        biguery_name = 'quotedataset'
        bigquery_description = 'this is a quotedataset test'
    
        client = bigquery.Client()
        dataset_ref = client.dataset(biguery_name)
        dataset = Dataset(dataset_ref)
        dataset.description = bigquery_description
        dataset = client.create_dataset(dataset)  # API request 

        print ('Dataset {} created.'.format(dataset.dataset_id))

        table_id = 'quotes_table'

        table_ref = dataset_ref.table(table_id)
        table = bigquery.Table(table_ref)

        table.schema = (
            bigquery.SchemaField('Quote', 'STRING', 'REPEATED'),
            bigquery.SchemaField('Author', 'STRING', 'REPEATED'),
            bigquery.SchemaField('Tags', 'STRING', 'REPEATED'),
        )

        table = client.create_table(table)
        print ('Created table {} in dataset {}.'.format(table_id, biguery_name))
    

    def load_data_from_file(source_file_name):
        
        biguery_name = 'quotedataset'
        table_id = 'quotes_table'
        
        bigquery_client = bigquery.Client()
        dataset_ref = bigquery_client.dataset(biguery_name)
        table_ref = dataset_ref.table(table_id)

        with open(source_file_name, 'rb') as source_file:
            # This example uses CSV, but you can use other formats.
            # See https://cloud.google.com/bigquery/loading-data
            job_config = bigquery.LoadJobConfig()
            #job_config.source_format = 'text/json'
            job_config.source_format = 'NEWLINE_DELIMITED_JSON'
            job_config.autodetect = True

            job = bigquery_client.load_table_from_file(
                source_file, table_ref, job_config=job_config)

        job.result()  # Waits for job to complete

        print ('Loaded {} rows into {}:{}.'.format(
            job.output_rows, biguery_name, table_id))
    


    def parse(self, response):
        for sel in response.css('div.quote'):
            item = MyQuotes()
            item['quote'] = sel.css('span.text::text').extract()
            item['author'] = sel.css('small.author::text').extract()
            item['tags'] = sel.css('.tag::text').extract()

            yield item
        

    #createBigQuery()
    load_data_from_file('result.jl')

    

        
