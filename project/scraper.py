# Thanks to: https://medium.com/analytics-vidhya/using-python-and-selenium-to-scrape-infinite-scroll-web-pages-825d12c24ec7
import redis
import os
from time import sleep
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
from selenium import webdriver
# from selenium.webdriver.chrome.service import Service # Not currently needed
from selenium.webdriver.chrome.options import Options

# Setup Chrome options
CHROME_OPTIONS = Options()
CHROME_OPTIONS.add_argument("--headless")  # Ensure it runs in headless mode
CHROME_OPTIONS.add_argument("--no-sandbox")  # Bypass OS security model, REQUIRED for Docker
CHROME_OPTIONS.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems

class WebCrawler:
    def __init__(self, url_base):

        self.url_base = url_base
        self.page_num = 1

        ###### Set up the Selenium webdriver ######
        self.webdriver = webdriver.Chrome(options=CHROME_OPTIONS)
        self.webdriver.get(self.url_base)
        sleep(2) # Allow time for the web page to open
        self.webdriver_scroll_pause_time = 1
        self.webdriver_screen_height = self.webdriver.execute_script("return window.screen.height;")   # Get the screen height


        ###### Initialize Elasticsearch ######
        username = 'elastic'
        password = os.getenv('ELASTIC_PASSWORD') # should be 'elastic' for now. make sure it's in the terminal environment.
        self.es_client = Elasticsearch(
           "http://localhost:9200",
           basic_auth=(username, password)
        )
        print(self.es_client.info()) # Test


        ###### Initialize Redis ######
        self.r = redis.Redis()
        self.r.flushall()


        # Run the crawler on initialization
        self.run(url_base)


    def run(self, starting_url):
        print(f"Starting crawl of domain: {self.url_base}")

        # Add root url as the entrypoint to our crawl
        self.r.lpush("links", starting_url)

        # Start crawl
        while link := self.r.rpop("links"):
            self.crawl(link)
            sleep(1) # Be nice to the server
        print("No more links to crawl!")


    def scroll_page(self, num_pages_to_scroll=5):
        # Scroll down num_pages screens(pages) max of html content each time this method is called
        print("Scrolling {self.webdriver.current_url}")
        stopping_point = self.page_num + num_pages_to_scroll
        while self.page_num < stopping_point:
            print(f"\tPage: {self.page_num}")
            # Scroll one screen height each time
            self.webdriver.execute_script("window.scrollTo(0, {screen_height}*{i});".format(screen_height=self.webdriver_screen_height, i=self.page_num))  
            self.page_num += 1
            sleep(self.webdriver_scroll_pause_time)
            # Update scroll height each time after scrolled, as the scroll height can change after we scrolled the page
            scroll_height = self.webdriver.execute_script("return document.body.scrollHeight;")
            # Break the loop when the height we need to scroll to is larger than the total scroll height
            if (self.webdriver_screen_height) * self.page_num > scroll_height:
                break


    def write_to_elastic(self, decoded_url, html):
        # Keeping this function separate for readability
        self.es_client.index(index='webpages', document={ 'url': decoded_url, 'html': html })


    def scrape(self):
        self.scroll_page()
        soup = BeautifulSoup(self.webdriver.page_source, "html.parser") # This can probably be done without creating the object multiple times

        # Extract URLs
        # for a_tag in soup.find_all("a"):
        #     link = a_tag.attrs['href']
        #     url = self.url_base + link
        #     print(url)

        a_tags = soup.find_all("a")
        hrefs = [ a.get("href") for a in a_tags ]

        # Do domain specific URL filtering
        links = [ self.url_base + a for a in hrefs if a and self.check_filters(a) ]

        # Put urls in Redis queue
        # create a linked list in Redis, call it "links"
        self.r.lpush("links", *links)


    def crawl(self, url):

        # because Redis stores strings as ByteStrings,
        # we must decode our url into a string of a specific encoding
        # for it to be valid JSON
        # We will keep the decoded url here to prevent repetitive decoding
        decoded_url = url.decode('utf-8')

        self.scrape()

        # Cache page to elasticsearch
        self.write_to_elastic(decoded_url, str(self.webdriver.current_url))


    def check_filters(self, href):
       return href.startswith("/articles/") or href.startswith("/news/")


WebCrawler("https://www.ign.com/")