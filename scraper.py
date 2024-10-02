# scraper.py
import scrapy
from scrapy.crawler import CrawlerProcess
from parser import clean_html
from extractor import extract_data
from save_data import save_to_csv
import base64

class EmployeeSpider(scrapy.Spider):
    name = "employee_spider"
    custom_settings = {
        'ROBOTSTXT_OBEY': True,
        'DOWNLOAD_DELAY': 0.5,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 750,
        },
    }

    def __init__(self, start_url, csv_file, *args, **kwargs):
        super(EmployeeSpider, self).__init__(*args, **kwargs)
        self.start_urls = [start_url]
        self.csv_file = csv_file

        # Proxy details
        self.proxy_host = "brd.superproxy.io:22225"
        self.proxy_user = "brd-customer-hl_117e4e9a-zone-web_unlocker1"
        self.proxy_pass = "c2ubbrdrpdfi"

    def start_requests(self):
        for url in self.start_urls:
            request = scrapy.Request(url=url, callback=self.parse)

            # Set the proxy
            request.meta['proxy'] = f"http://{self.proxy_host}"

            # Encode the proxy credentials
            proxy_user_pass = f"{self.proxy_user}:{self.proxy_pass}"
            encoded_user_pass = base64.b64encode(proxy_user_pass.encode('utf-8')).decode('utf-8')

            # Set the Proxy-Authorization header
            request.headers['Proxy-Authorization'] = 'Basic ' + encoded_user_pass

            yield request

    def parse(self, response):
        # Clean the HTML content
        html_content = response.text
        cleaned_text = clean_html(html_content)

        # Extract data using OpenAI API
        json_data = extract_data(cleaned_text)

        # Save data to CSV
        save_to_csv(json_data, self.csv_file)
