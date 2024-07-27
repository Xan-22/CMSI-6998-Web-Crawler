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

        print(f"Initializing WebCrawler for domain: {url_base}")
        self.url_base = url_base
        self.page_num = 1

        ###### Set up the Selenium webdrivers ######
        self.base_wd = webdriver.Chrome(options=CHROME_OPTIONS)
        self.base_wd.get(self.url_base)
        print("Initialized Base WebDriver")
        self.article_wd = webdriver.Chrome(options=CHROME_OPTIONS)
        self.article_wd.get(self.url_base)
        print("Initialized Article WebDriver")
        sleep(2) # Allow time for the web pages to open

        self.webdriver_scroll_pause_time = 1
        self.webdriver_screen_height = self.base_wd.execute_script("return window.screen.height;")  # Get the screen height


        ###### Initialize Elasticsearch ######
        username = 'elastic'
        password = os.getenv('ELASTIC_PASSWORD') # should be 'elastic' for now. make sure it's in the terminal environment.
        self.es_client = Elasticsearch(
           "http://localhost:9200",
           basic_auth=(username, password)
        )
        print(self.es_client.info()) # Test

        ###### Initialize Redis ######
        print("Initializing Redis cache")
        self.r = redis.Redis()
        self.r.flushall()

        # Run the crawler on initialization
        print("Initialization complete. Running crawler...")
        self.run(url_base)


    def run(self, starting_url):
        print(f"Starting crawl of domain: {self.url_base}")

        # Start crawl
        self.start_crawl(starting_url)

        print("No more links to crawl!")


    def scroll_page(self, webdriver, num_pages_to_scroll=5):
        # Scroll down num_pages screens(pages) max of html content each time this method is called
        print("Scrolling {webdriver.current_url}")
        stopping_point = self.page_num + num_pages_to_scroll
        while self.page_num < stopping_point:
            print(f"\tPage: {self.page_num}")
            # Scroll one screen height each time
            webdriver.execute_script("window.scrollTo(0, {screen_height}*{i});".format(screen_height=self.webdriver_screen_height, i=self.page_num))  
            self.page_num += 1
            sleep(self.webdriver_scroll_pause_time)
            # Update scroll height each time after scrolled, as the scroll height can change after we scrolled the page
            scroll_height = webdriver.execute_script("return document.body.scrollHeight;")
            # Break the loop when the height we need to scroll to is larger than the total scroll height
            if (self.webdriver_screen_height) * self.page_num > scroll_height:
                break


    def write_to_elastic_webpages(self, decoded_url, html):
        # Keeping this function separate for readability
        #self.es_client.index(index='webpages', document={ 'url': decoded_url, 'html': html })
        return


    def write_to_elastic_articles(self, headline, date, author, body):
        # Keeping this function separate for readability
        # TODO: Get article headline
        #self.es_client.index(index='articles', document={ 'headline': headline, 'date': date, 'author': author, 'body': body })
        return


    def start_crawl(self, url):

        # Start scrolling and scrape for links
        self.scrape_links(url, self.base_wd)

        # Scrape each article
        while link := self.r.rpop("links"):
            self.scrape_data(self.url_base, link, self.article_wd)
            sleep(1) # Be nice to the server


    def scrape_links(self, url_base, webdriver):
        print(f"Scraping Links from: {url_base}")
        self.scroll_page(webdriver, 5)
        soup = BeautifulSoup(webdriver.page_source, "html.parser") # This can probably be done without creating the object multiple times

        a_tags = soup.find_all("a")
        hrefs = [ a.get("href") for a in a_tags ]

        # Do domain specific URL filtering
        links = [ self.url_base + a for a in hrefs if a and self.check_filters(url_base, a) ]

        # Put urls in Redis queue
        # create a linked list in Redis, call it "links"
        self.r.lpush("links", *links)

    
    def scrape_data(self, url_base, url, webdriver):
        print (f"Scraping Data from: {url}")
        self.scroll_page(webdriver)
        soup = BeautifulSoup(self.article_wd.page_source, "html.parser")

        # Cache page to elasticsearch
        self.write_to_elastic_webpages(url.decode('utf-8'), str(self.base_wd.current_url))

        self.scrape_article_data(url_base, soup)
        

    def scrape_article_data(self, url_base, soup):
        match (url_base): # TODO: Add more sites
            case "https://www.ign.com/":
                headline = soup.find("h1").get_text()
                date = soup.find("time").get_text()
                author = soup.find("a", class_="author").get_text()
                body = soup.find("article").get_text()
            #case "https://www.gameinformer.com/":
            #case "https://www.pcgamer.com/":
            case default: # Should not happen
                return
            
        self.write_to_elastic_articles(headline, date, author, body)


    def check_filters(self, url_base, href):
       # These cover most of our desired articles' URL patterns 
       # on sites with infinitely scrolling main pages
       # such as IGN, GameInformer, PCGamer, etc. 
        match (url_base):
            case "https://www.ign.com/":
                return (href.startswith("/articles/") or 
                       href.startswith("/news/") or
                       href.startswith("/review/") or
                       href.startswith("/exclusive/") or
                       href.startswith("/preview/") or
                       href.startswith("/games/") or
                       href.startswith("/gaming-industry/"))
            case "https://www.gameinformer.com/":
                return (href.startswith("/news/") or 
                       href.startswith("/preview/") or
                       href.startswith("/review/") or
                       href.startswith("/feature/") or
                       href.startswith("/blog/") or
                       href.startswith("/video/"))
            case default: # Stick to chosen sites
                return False


# Primarily using these three for testing right now
WebCrawler("https://www.ign.com/")
#WebCrawler("https://www.gameinformer.com/")
#WebCrawler("https://www.pcgamer.com/")