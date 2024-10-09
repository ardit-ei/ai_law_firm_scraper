import scrapy
import json
import hashlib
import re
import random
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy_playwright.page import PageMethod
from parser import clean_html
from extractor import extract_data
from save_data import save_to_csv

class EmployeeSpider(CrawlSpider):
    name = "employee_spider"
    custom_settings = {
        'CONCURRENT_REQUESTS': 5,  # Reduce concurrent requests
        'CONCURRENT_REQUESTS_PER_DOMAIN': 3,  # Limit requests per domain
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': random.uniform(1, 3),
        'AUTOTHROTTLE_MAX_DELAY': random.uniform(5, 15),
        'DOWNLOAD_DELAY': random.uniform(0.5, 2.0),
        'ROBOTSTXT_OBEY': False,
        'COOKIES_ENABLED': True,
        'RETRY_HTTP_CODES': [403, 429, 500, 502, 503, 504],
        'RETRY_TIMES': 5,
        'DEPTH_LIMIT': 2,  # Limit depth to avoid unnecessary crawls
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': True,
            'timeout': 30000,
            'args': ['--ignore-certificate-errors'],  # Ignore SSL issues
        },
        'DOWNLOAD_HANDLERS': {
            'http': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
            'https': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
        },
        'HTTPCACHE_ENABLED': False,  # Disable HTTP cache
        'REDIRECT_ENABLED': True,    # Ensure redirects are enabled
        'LOG_LEVEL': 'DEBUG',        # Increase log level for debugging
    }

    def __init__(self, start_url, csv_file, *args, **kwargs):
        super(EmployeeSpider, self).__init__(*args, **kwargs)
        self.start_urls = [start_url]
        self.csv_file = csv_file
        self.allow_domain = start_url.split("//")[-1].split("/")[0]
        self.allowed_domains = [self.allow_domain]  # Enforce domain restriction
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

    def block_unwanted_requests(self, route, request):
        allowed_resource_types = ['document', 'xhr', 'fetch', 'script']  # Allow only necessary types
        blocked_extensions = ['.svg', '.woff', '.woff2', '.ttf', '.eot', '.otf', '.css', '.png', '.jpg', '.jpeg', '.gif', '.bmp']
        
        self.logger.debug(f"Request Resource Type: {request.resource_type}, URL: {request.url}")
        
        # Block if resource type is not allowed
        if request.resource_type not in allowed_resource_types:
            self.logger.info(f"Blocking resource type: {request.resource_type} | URL: {request.url}")
            return route.abort()
        
        # Block based on URL extensions
        if any(request.url.lower().endswith(ext) for ext in blocked_extensions):
            self.logger.info(f"Blocking resource extension: {request.url}")
            return route.abort()
        
        return route.continue_()

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse_page,
                meta={
                    "playwright": True,  # Enable Playwright to capture the final redirected URL
                    "playwright_page_methods": [
                        PageMethod("wait_for_load_state", "networkidle"),  # Wait for all network activity to finish
                        PageMethod("route", "**/*", self.block_unwanted_requests)
                    ],
                    "record_page": True,  # Allow access to the page object in the callback
                    "handle_httpstatus_list": [301, 302, 303, 307, 308],
                    "dont_redirect": False,  # Allow redirects
                },
                errback=self.handle_error,
                dont_filter=True  # Force processing even if URLs are similar
            )


    def parse_page(self, response):
        # Handle redirects manually
        if response.status in [301, 302, 303, 307, 308]:
            redirect_url = response.headers.get('Location')
            if redirect_url:
                redirect_url = response.urljoin(redirect_url.decode('utf-8'))
                self.logger.info(f"Redirecting to: {redirect_url}")
                yield scrapy.Request(
                    url=redirect_url,
                    callback=self.parse_page,
                    meta=response.meta,
                    dont_filter=True
                )
                return

        # Access the Playwright page object
        page = response.meta.get('playwright_page')
        if page:
            final_url = page.url
            page.close()  # Close the page to prevent memory leaks
        else:
            final_url = response.url
        self.logger.info(f"Processing final URL after redirects: {final_url}")

        # Normalize the final URL to avoid duplicates
        normalized_url = self.normalize_url(final_url)
        if normalized_url in self.visited_urls:
            return
        self.visited_urls.add(normalized_url)

        # Check if the final URL matches predefined slugs for employee pages
        if any(slug in final_url.lower() for slug in [
            '/bio/', '/our-people/', '/attorneys/', '/attorney/', '/people/', '/team/', '/our-team/',
            '/lawyer/', '/lawyers/', '/professionals/', '/professional/', '/profiles/', '/profile/',
            '/meet-our-team/', '/our-firm/', '/attorney-profiles/', '/staff-counsel/', '/staff/',
            '/counsel/', '.attorneys/'
        ]):
            self.logger.info(f"Processing content from URL: {final_url}")
            
            html_content = response.text
            cleaned_text = clean_html(html_content)

            # Extract data using OpenAI API
            json_data = extract_data(cleaned_text)

            if json_data:  # Ensure extracted data is not empty
                # Add scraped URL to the data
                json_data = self.add_scraped_url(json_data, final_url)

                # Validate data before saving
                if self.validate_data_in_content(json_data, cleaned_text):
                    save_to_csv(json_data, self.csv_file)
                else:
                    self.logger.warning(f"Validation failed for {final_url}: Email or phone not found")
            else:
                self.logger.warning(f"No data extracted from {final_url}")

        # Follow links from this page
        link_extractor = LinkExtractor(
            allow_domains=[self.allow_domain],
            deny=[r'/blog/', r'/news/'],
            unique=True
        )
        links = link_extractor.extract_links(response)
        for link in links:
            normalized_link = self.normalize_url(link.url)
            if normalized_link not in self.visited_urls:
                yield scrapy.Request(
                    url=link.url,
                    callback=self.parse_page,
                    meta={
                        "playwright": True, 
                        "playwright_page_methods": [
                            PageMethod("wait_for_load_state", "networkidle"),
                            PageMethod("route", "**/*", self.block_unwanted_requests)
                        ],
                        "record_page": True,
                    }
                )

    def handle_error(self, failure):
        """Handle request errors by adjusting scraping level."""
        if failure.check(HttpError):
            response = failure.value.response
            if response.status in [403, 429]:
                self.logger.warning(f"Bot protection detected at {response.url}, retrying with Playwright.")
                yield scrapy.Request(
                    url=response.url,
                    callback=self.parse_page,
                    meta={
                        "playwright": True,
                        "playwright_page_methods": [
                            PageMethod("wait_for_load_state", "networkidle"),
                            PageMethod("route", "**/*", self.block_unwanted_requests)
                        ],
                        "record_page": True,
                    },
                    errback=self.handle_error,
                    dont_filter=True  # Ensure the request isn't filtered out
                )

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

    def normalize_url(self, url):
        """Normalize URLs to avoid crawling duplicates."""
        # Strip query parameters and fragments to avoid re-crawling the same page
        parsed_url = url.split('?')[0].split('#')[0]
        return hashlib.md5(parsed_url.lower().encode()).hexdigest()
