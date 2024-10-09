# main.py
import sys
import asyncio
from twisted.internet import asyncioreactor  # Import the correct reactor
asyncioreactor.install()  # Install AsyncioSelectorReactor

from scraper import EmployeeSpider
from scrapy.crawler import CrawlerProcess

def run_spider(start_url, csv_file):
    process = CrawlerProcess()
    process.crawl(EmployeeSpider, start_url=start_url, csv_file=csv_file)
    process.start()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python main.py <start_url> <output_csv_file>")
        sys.exit(1)

    start_url = sys.argv[1]
    csv_file = sys.argv[2]
    run_spider(start_url, csv_file)
