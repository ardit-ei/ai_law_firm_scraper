import scrapy
import json
import hashlib
import re
import random
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.crawler import CrawlerProcess
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.chrome.service import Service  # Import Service class
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager  # webdriver-manager to auto-install ChromeDriver
from parser import clean_html
from extractor import extract_data
from save_data import save_to_csv

class EmployeeSpider(CrawlSpider):
    name = "employee_spider"
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': random.uniform(1, 3),
        'AUTOTHROTTLE_MAX_DELAY': random.uniform(5, 15),
        'DOWNLOAD_DELAY': random.uniform(0.5, 2.0),
        'ROBOTSTXT_OBEY': False,
        'COOKIES_ENABLED': True,
        'RETRY_HTTP_CODES': [403, 429, 500, 502, 503, 504],
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 750,
            'scrapy_selenium.SeleniumMiddleware': 800,
        },
        'RETRY_TIMES': 5,
        'SELENIUM_DRIVER_NAME': 'chrome',
        'SELENIUM_DRIVER_ARGUMENTS': ['--headless', '--no-sandbox', '--disable-dev-shm-usage'],
        'DEPTH_LIMIT': 2,
        'HTTPPROXY_ENABLED': True,
        'HTTP_PROXY': 'http://brd-customer-hl_117e4e9a-zone-web_unlocker1:c2ubbrdrpdfi@brd.superproxy.io:22225',
    }

    def __init__(self, start_url, csv_file, *args, **kwargs):
        super(EmployeeSpider, self).__init__(*args, **kwargs)
        self.start_urls = [start_url]
        self.csv_file = csv_file
        self.allow_domain = start_url.split("//")[-1].split("/")[0]
        self.visited_urls = set()

        # Define link extraction rules
        self.rules = (
            Rule(
                LinkExtractor(
                    allow_domains=[self.allow_domain],
                    deny=[r'/blog/', r'/news/'],
                    unique=True
                ),
                callback='parse_page',
                follow=True
            ),
        )
        self._compile_rules()

    def start_requests(self):
        for url in self.start_urls:
            yield SeleniumRequest(
                url=url,
                callback=self.parse_page,
                wait_time=15,
                wait_until=EC.presence_of_element_located((By.TAG_NAME, "body")),
                meta={'depth': 0}  # Start at depth 0
            )

    def simulate_human_interaction(self, driver):
        """Simulate human-like interaction with the page to avoid detection."""
        actions = ActionChains(driver)
        actions.move_by_offset(200, 100).perform()  # Simulate mouse movement
        actions.pause(random.uniform(1, 3)).perform()  # Pause for a random time

    def parse_page(self, response):
        # Access driver from response meta safely
        driver = response.meta.get('driver')
        if driver:
            self.simulate_human_interaction(driver)

        # Proceed with your normal scraping logic
        html_content = response.text
        cleaned_text = clean_html(html_content)

        # Extract data using OpenAI API
        json_data = extract_data(cleaned_text)

        # Ensure json_data is parsed correctly if it’s a string
        if isinstance(json_data, str):
            try:
                json_data = json.loads(json_data)
            except json.JSONDecodeError:
                self.logger.error(f"Failed to decode JSON from {response.url}. Skipping...")
                return

        if json_data:
            # Validate that emails and phone numbers are actually on the page
            if self.validate_data_in_content(json_data, cleaned_text):
                # Add scraped URL to the data
                json_data = self.add_scraped_url(json_data, response.url)
                
                # Save to CSV
                save_to_csv(json_data, self.csv_file)
            else:
                self.logger.warning(f"Validation failed: Email or phone not found on {response.url}")
        else:
            self.logger.warning(f"No data extracted from {response.url}")

        # If the depth is less than 2, follow links on the page
        if response.meta.get('depth', 0) < 2:
            for link in LinkExtractor(
                allow_domains=[self.allow_domain],
                deny=[r'/blog/', r'/news/'],
                unique=True
            ).extract_links(response):
                normalized_link = self.normalize_url(link.url)
                if normalized_link not in self.visited_urls:
                    yield SeleniumRequest(
                        url=link.url,
                        callback=self.parse_page,
                        wait_time=15,
                        wait_until=EC.presence_of_element_located((By.TAG_NAME, "body")),
                        meta={'depth': response.meta.get('depth', 0) + 1}
                    )

    def normalize_url(self, url):
        return hashlib.md5(url.lower().encode()).hexdigest()

    def add_scraped_url(self, json_data, url):
        """Helper method to add scraped URL to the extracted JSON data."""
        if isinstance(json_data, dict) and 'person' in json_data:
            json_data['scraped_url'] = url
        elif isinstance(json_data, str):
            try:
                person_data = json.loads(json_data).get('person', {})
                json_data = {
                    'scraped_url': url,
                    'person': person_data
                }
            except json.JSONDecodeError:
                self.logger.error("Invalid JSON data. Skipping...")
                return None
        else:
            self.logger.error("Extracted data does not contain 'person' key. Skipping...")
            return None
        return json_data

    def validate_data_in_content(self, json_data, page_content):
        """Validate that the email and phone numbers are present in the cleaned HTML content."""
        person = json_data.get('person', {})
        email = person.get('email', '')
        direct_phone = person.get('direct_phone', '')
        mobile_phone = person.get('mobile_phone', '')

        # Validate email
        email_found = email and email in page_content and self.is_valid_email(email)
        if not email_found:
            person['email'] = ''  # Drop invalid email

        # Validate and format phone numbers (handle US phone formats)
        direct_phone_found = direct_phone and re.sub(r'\D', '', direct_phone) in re.sub(r'\D', '', page_content)
        if direct_phone_found:
            person['direct_phone'] = self.format_phone_number(direct_phone)

        mobile_phone_found = mobile_phone and re.sub(r'\D', '', mobile_phone) in re.sub(r'\D', '', page_content)
        if mobile_phone_found:
            person['mobile_phone'] = self.format_phone_number(mobile_phone)

        return email_found or direct_phone_found or mobile_phone_found

    def is_valid_email(self, email):
        """Validate email format using a regular expression."""
        email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
        return re.match(email_regex, email) is not None

    def format_phone_number(self, phone):
        """Format a variety of US phone number formats into 000-000-0000."""
        digits = re.sub(r'\D', '', phone)  # Remove all non-digit characters
        if len(digits) == 10:
            return f'{digits[:3]}-{digits[3:6]}-{digits[6:]}'
        elif len(digits) == 11 and digits.startswith('1'):
            return f'{digits[1:4]}-{digits[4:7]}-{digits[7:]}'
        else:
            return phone  # Return the original if it's not a valid US number

# Run the spider
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python3 scraper.py <start_url> <output_csv_file>")
    else:
        start_url = sys.argv[1]
        output_csv_file = sys.argv[2]
        process = CrawlerProcess()
        process.crawl(EmployeeSpider, start_url=start_url, csv_file=output_csv_file)
        process.start()
