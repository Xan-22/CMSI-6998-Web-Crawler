# Thanks to: https://medium.com/analytics-vidhya/using-python-and-selenium-to-scrape-infinite-scroll-web-pages-825d12c24ec7
import redis
import os
import time
import hashlib
from datetime import datetime
from bs4 import BeautifulSoup
from threading import Thread
from enum import Enum
from elasticsearch import Elasticsearch
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# !! Set ES Cloud ID and API Key Here
ES_CLOUD_ID = os.getenv('ELASTIC_CLOUD_ID')
ES_API_KEY = os.getenv('ELASTIC_API_KEY')

# Enum for supported sites
SITE = Enum('SITE', 
            [
                ('IGN', "https://www.ign.com/"), 
                ('GameInformer', "https://www.gameinformer.com/"), # RIP GameInformer
                ('PCGamer', "https://www.pcgamer.com/")
                ]
            )

# Setup Chrome options
CHROME_OPTIONS = Options()
CHROME_OPTIONS.add_argument("--headless")  # Ensure it runs in headless mode
CHROME_OPTIONS.add_argument("--no-sandbox")  # Bypass OS security model, REQUIRED for Docker
CHROME_OPTIONS.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems


# Helper function to generate a hash ID for articles
def generate_hash_id(site, headline, date):
    combined_key = site + headline + date
    return hashlib.md5(combined_key.encode('utf-8')).digest()


class WebCrawler:
    def __init__(self, site, subdirectory=""):

        self.site = site
        self.subdirectory = subdirectory
        self.id = f"{self.site.name + self.subdirectory}"
        print(f"{self.id}: Initializing WebCrawler for domain: {self.main_page}")
        self.page_num = 1
        self.has_links = True

        ###### Set up the Selenium webdrivers ######
        self.base_wd = webdriver.Chrome(options=CHROME_OPTIONS)
        self.base_wd.get(self.site.value)
        print(f"{self.id}: Initialized Base WebDriver")
        self.article_wd = webdriver.Chrome(options=CHROME_OPTIONS)
        self.article_wd.get(self.site.value)
        print(f"{self.id}: Initialized Article WebDriver")
        time.sleep(2) # Allow time for the web pages to open

        self.webdriver_scroll_pause_time = 1
        self.webdriver_screen_height = self.base_wd.execute_script("return window.screen.height;")  # Get the screen height


        ###### Initialize Elasticsearch client ######
        self.es_client = Elasticsearch(
            ES_CLOUD_ID,
            api_key=ES_API_KEY
        )
        print(f"{self.id}: {self.es_client.info()}") # Test


        ###### Initialize Redis client ######
        print(f"{self.id}: Initializing Redis cache")
        self.r = redis.Redis()
        self.r.delete(f"{self.id}-links")

        # Run the crawler on initialization
        print(f"{self.id}: Initialization complete. Running crawler...")
        self.run()


    def run(self):
        print(f"{self.id}: Starting crawl of domain: {self.site.value + self.subdirectory}")

        # Start crawl
        while(self.has_links):
            self.start_crawl()

        print(f"{self.id}: No more links to crawl!")
        self.base_wd.quit()
        self.article_wd.quit()


    def start_crawl(self):

        # Start scrolling and scrape for links
        self.page_num = self.scroll_page(self.base_wd, self.page_num)
        self.extract_links(self.site.value + self.subdirectory, self.base_wd)

        # Scrape each article
        while link := self.r.rpop(f"{self.id}-links"):
            self.scrape(link)
            print(f"{self.id}: Scraped. Waiting a few seconds...")
            time.sleep(3) # Be REALLY nice to the server


    def scroll_page(self, webdriver, page_num, num_pages_to_scroll=3):
        # Scroll down num_pages screens(pages) max of html content each time this method is called
        print(f"{self.id}: Scrolling {webdriver.current_url}")
        stopping_point = page_num + num_pages_to_scroll
        while page_num < stopping_point:
            print(f"{self.id}: \tPage: {page_num}")
            # Scroll one screen height each time
            webdriver.execute_script("window.scrollTo(0, {screen_height}*{i});".format(screen_height=self.webdriver_screen_height, i=page_num))  
            page_num += 1
            time.sleep(self.webdriver_scroll_pause_time)
            # Update scroll height each time after scrolled, as the scroll height can change after we scrolled the page
            scroll_height = webdriver.execute_script("return document.body.scrollHeight;")
            # Break the loop when the height we need to scroll to is larger than the total scroll height
            if (self.webdriver_screen_height) * page_num > scroll_height:
                break
        return stopping_point # new page number


    def scrape(self, url):
        decoded_url = url.decode('utf-8')
        self.article_wd.get(decoded_url)
        soup = BeautifulSoup(self.article_wd.page_source, "html.parser")
        print (f"{self.id}: Scraping Data from: {decoded_url}")

        try:
            self.scrape_article_data(soup)
            # Cache article URLs to Elasticsearch
            self.write_to_elastic_webpages(decoded_url, str(self.base_wd.current_url))
        except Exception as e:
            print(f"{self.id}: Invalid article format: {e}")
            self.scroll_page(self.article_wd, 1)
            self.extract_links(url, self.article_wd) # Try to find links on the page if it isn't an article


    def scrape_article_data(self, soup):
        match (self.site):
            case SITE.IGN:
                headline = soup.find("h1").get_text()
                authors = [ a.get_text() if a is not None else "N/A" for a in soup.find_all("a", class_="jsx-3953721931 article-author underlined") ]
                date = datetime.strptime(soup.find("meta", attrs={"property" : "article:published_time"}).get("content").split("T")[0], "%Y-%m-%d").strftime("%Y-%m-%d")
                body = soup.find_all("p", class_="jsx-3649800006")
                topics = [ t.get_text() if t is not None else "N/A" for t in soup.find_all("a", attrs={"data-cy": "object-breadcrumb"}) ]
            case SITE.GameInformer:
                headline = soup.find("h1", class_="page-title").get_text()
                authors = [ a.get_text() if a is not None else "N/A" for a in soup.find("div", class_="author-details").find_all("a") ]
                date = datetime.strptime(soup.find("div", class_="author-details").get_text().split("on ")[1], "%b %d, %Y at %I:%M %p").strftime("%Y-%m-%d")
                body = soup.find("div", class_="ds-main").find_all("p")
                topics = [ t.get_text().strip("\n") if t is not None else "N/A" for t in soup.find("div", class_="gi5--product--summary").find_all("a", attrs={"rel": "bookmark"}) ]
            case SITE.PCGamer:
                headline = soup.find("h1").get_text()
                authors = [ a.get_text() if a is not None else "N/A" for a in soup.find("div", class_="author-byline__authors").find_all("a", class_="link author-byline__link") ]
                date = datetime.strptime(soup.find("meta", attrs={"name": "pub_date"}).get("content").split("T")[0], "%Y-%m-%d").strftime("%Y-%m-%d")
                body = soup.find("div", attrs={"id" : "article-body"}).find_all("p", class_=None)
                topics = [ t.find("a").get_text().strip("\n") if t is not None else "N/A" for t in soup.find_all("div", class_="tag", attrs={"data-analytics-id" : "article-product"}) ]
        body_text = ""
        for p in body:
            body_text += p.get_text() + "\n"
        self.write_to_elastic_articles(self.site.name, headline, date, authors, body_text, topics)


    def extract_links(self, url, webdriver):
        print(f"{self.id}: Scraping Links from: {url}")

        attempts = 0
        links = []
        while (len(links) == 0 and attempts < 3):
            attempts += 1
            time.sleep(5) # Wait for the page to load
            soup = BeautifulSoup(webdriver.page_source, "html.parser") # This can probably be done without creating the object multiple times
            a_tags = soup.find_all("a")
            hrefs = [ a.get("href") for a in a_tags ]

            # Do domain specific URL filtering
            match (self.site):
                case SITE.PCGamer: # PCGamer puts the whole URL in their hrefs
                    print(f"{self.id}: PCGamer URL filtering")
                    filtered = [ a for a in hrefs if a and self.check_filters(a) and self.es_client.exists(index='webpages', id=a) == False ]
                case _:
                    print(f"{self.id}: Generic URL filtering")
                    filtered = [ url + a for a in hrefs if a and self.check_filters(a) and self.es_client.exists(index='webpages', id=(url + a)) == False ]
            links = list(set(filtered)) # Remove duplicates
            print(f"{self.id}: Found {len(links)} links")
        if (len(links) > 0):
            # Put links into a queue in Redis, call it "(name)-links"
            self.r.lpush(f"{self.id}-links", *links)
        else:
            print(f"{self.id}: No links found after 5 attempts. Exiting.")
            self.has_links = False


    def write_to_elastic_webpages(self, decoded_url, domain):
        self.es_client.index(index='webpages', id=decoded_url, document={ 'url': decoded_url, 'domain': domain })


    def write_to_elastic_articles(self, site, headline, date, authors, body, topics):
        self.es_client.index(
            index='articles', 
            id = generate_hash_id(site, headline, date),
            document={ 
                'site': site, 
                'headline': headline, 
                'date': date, 
                'author': authors, 
                'body': body, 
                'topics': topics 
            }
        )


    def check_filters(self, href):
       # These cover most of our desired articles' URL patterns 
       # on sites with infinitely scrolling main pages
       # such as IGN, GameInformer, PCGamer, etc. 
        match (self.site):
            case SITE.IGN:
                return (href.startswith("/articles/"))
            case SITE.GameInformer:
                return (href.startswith("/news/") or 
                        href.startswith("/preview/") or
                        href.startswith("/review/") or
                        href.startswith("/feature/") or
                        href.startswith("/blog/") or
                        href.startswith("/gamer-culture/"))
            case SITE.PCGamer:
                return (href.startswith(SITE.PCGamer.value))
            case _: # Stick to chosen sites
                return False


def start_webcrawler(url_base, iden):
    print(f"{iden}: Started at {time.strftime('%X')}")
    WebCrawler(url_base, iden)
    print(f"{iden}: Finished at {time.strftime('%X')}")


def main():
    thread1 = Thread(target=start_webcrawler, args=(SITE.IGN, "/news/"))
    thread2 = Thread(target=start_webcrawler, args=(SITE.IGN, "/reviews/"))
    thread3 = Thread(target=start_webcrawler, args=(SITE.PCGamer, "/games/"))
    thread4 = Thread(target=start_webcrawler, args=(SITE.PCGamer, "/archive/"))
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()

main()