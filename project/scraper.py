# Thanks to: https://medium.com/analytics-vidhya/using-python-and-selenium-to-scrape-infinite-scroll-web-pages-825d12c24ec7
import redis
import os
import time
from datetime import datetime
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from threading import Thread

# Setup Chrome options
CHROME_OPTIONS = Options()
CHROME_OPTIONS.add_argument("--headless")  # Ensure it runs in headless mode
CHROME_OPTIONS.add_argument("--no-sandbox")  # Bypass OS security model, REQUIRED for Docker
CHROME_OPTIONS.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems

class WebCrawler:
    def __init__(self, url_base, site_name):

        self.iden = site_name
        print(f"{self.iden}: Initializing WebCrawler for domain: {url_base}")
        self.url_base = url_base
        self.page_num = 1
        self.has_links = True

        ###### Set up the Selenium webdrivers ######
        self.base_wd = webdriver.Chrome(options=CHROME_OPTIONS)
        self.base_wd.get(self.url_base)
        print(f"{self.iden}: Initialized Base WebDriver")
        self.article_wd = webdriver.Chrome(options=CHROME_OPTIONS)
        self.article_wd.get(self.url_base)
        print(f"{self.iden}: Initialized Article WebDriver")
        time.sleep(2) # Allow time for the web pages to open

        self.webdriver_scroll_pause_time = 1
        self.webdriver_screen_height = self.base_wd.execute_script("return window.screen.height;")  # Get the screen height


        ###### Initialize Elasticsearch client ######
        es_username = 'elastic'
        es_password = os.getenv('ELASTIC_PASSWORD')
        es_cloud_id = os.getenv('ELASTIC_CLOUD_ID')

        self.es_client = Elasticsearch(
            # TODO: ENTER YOUR ELASTICSEARCH CLOUD ID AND API KEY HERE
            "",
            api_key=""
           #"http://localhost:9200", # For local testing
           #basic_auth=(es_username, es_password)
        )
        print(f"{self.iden}: {self.es_client.info()}") # Test


        ###### Initialize Redis client ######
        print(f"{self.iden}: Initializing Redis cache")
        self.r = redis.Redis()
        self.r.delete(f"{self.iden}-links")

        # Run the crawler on initialization
        print(f"{self.iden}: Initialization complete. Running crawler...")
        self.run(url_base)


    def run(self, starting_url):
        print(f"{self.iden}: Starting crawl of domain: {self.url_base}")

        # Start crawl
        while(self.has_links):
            self.start_crawl(starting_url)

        print(f"{self.iden}: No more links to crawl!")
        self.base_wd.quit()
        self.article_wd.quit()


    def scroll_page(self, webdriver, num_pages_to_scroll=1):
        # Scroll down num_pages screens(pages) max of html content each time this method is called
        print(f"{self.iden}: Scrolling {webdriver.current_url}")
        stopping_point = self.page_num + num_pages_to_scroll
        while self.page_num < stopping_point:
            print(f"{self.iden}: \tPage: {self.page_num}")
            # Scroll one screen height each time
            webdriver.execute_script("window.scrollTo(0, {screen_height}*{i});".format(screen_height=self.webdriver_screen_height, i=self.page_num))  
            self.page_num += 1
            time.sleep(self.webdriver_scroll_pause_time)
            # Update scroll height each time after scrolled, as the scroll height can change after we scrolled the page
            scroll_height = webdriver.execute_script("return document.body.scrollHeight;")
            # Break the loop when the height we need to scroll to is larger than the total scroll height
            if (self.webdriver_screen_height) * self.page_num > scroll_height:
                break


    def write_to_elastic_webpages(self, decoded_url, domain):
        self.es_client.index(index='webpages', document={ 'url': decoded_url, 'domain': domain })


    def write_to_elastic_articles(self, site, headline, date, author, body, topics):
        self.es_client.index(index='articles', document={ 'site': site, 'headline': headline, 'date': date, 'author': author, 'body': body, 'topics': topics })


    def start_crawl(self, url):

        # Start scrolling and scrape for links
        self.scrape_links(url, self.base_wd)

        # Scrape each article
        while link := self.r.rpop(f"{self.iden}-links"):
            self.scrape_data(self.url_base, link)
            print(f"{self.iden}: Scraped. Waiting a few seconds...")
            time.sleep(3) # Be REALLY nice to the server


    def scrape_links(self, url_base, webdriver):
        print(f"{self.iden}: Scraping Links from: {url_base}")
        self.scroll_page(webdriver, 2)

        attempts = 0
        links = []
        while (len(links) == 0 and attempts < 5):
            attempts += 1
            time.sleep(2) # Wait for the page to load
            soup = BeautifulSoup(webdriver.page_source, "html.parser") # This can probably be done without creating the object multiple times
            a_tags = soup.find_all("a")
            hrefs = [ a.get("href") for a in a_tags ]

            # Do domain specific URL filtering
            match (url_base):
                case "https://www.pcgamer.com/": # PCGamer puts the whole URL in their hrefs
                    print(f"{self.iden}: PCGamer URL filtering")
                    filtered = [ a for a in hrefs if a and self.check_filters(url_base, a) and self.es_client.exists(index='webpages', id=a) == False ]
                case default:
                    print(f"{self.iden}: Generic URL filtering")
                    filtered = [ self.url_base + a for a in hrefs if a and self.check_filters(url_base, a) and self.es_client.exists(index='webpages', id=(self.url_base + a)) == False ]
            links = list(set(filtered)) # Remove duplicates
            print(f"{self.iden}: Found {len(links)} links")
        if (len(links) > 0):
            # Put links into a queue in Redis, call it "(name)-links"
            self.r.lpush(f"{self.iden}-links", *links)
        else:
            print(f"{self.iden}: No links found after 5 attempts. Exiting.")
            self.has_links = False
            return

    
    def scrape_data(self, url_base, url):
        decoded_url = url.decode('utf-8')
        self.article_wd.get(decoded_url)
        soup = BeautifulSoup(self.article_wd.page_source, "html.parser")
        print (f"{self.iden}: Scraping Data from: {decoded_url}")

        # Cache page to elasticsearch
        self.write_to_elastic_webpages(decoded_url, str(self.base_wd.current_url))

        self.scrape_article_data(url_base, soup)
        

    def scrape_article_data(self, url_base, soup):
        # TODO: We are throwing out a lot of articles that don't fit the assumed format.
        # We should substitute bad data for unknowns and store them as-is instead.
        try:
            match (url_base):
                case "https://www.ign.com/":
                    headline = soup.find("h1").get_text()
                    authors = [ a.get_text() for a in soup.find_all("a", class_="jsx-3953721931 article-author underlined") ]
                    date = datetime.strptime(soup.find("meta", attrs={"property" : "article:published_time"}).get("content").split("T")[0], "%Y-%m-%d").strftime("%Y-%m-%d")
                    body = soup.find_all("p", class_="jsx-3649800006")
                    body_text = ""
                    for p in body:
                        body_text += p.get_text() + "\n"
                    topics = [ t.get_text() for t in soup.find_all("a", attrs={"data-cy": "object-breadcrumb"}) ]
                case "https://www.gameinformer.com/":
                    headline = soup.find("h1", class_="page-title").get_text()
                    authors = [ a.get_text() for a in soup.find("div", class_="author-details").find_all("a") ]
                    date = datetime.strptime(soup.find("div", class_="author-details").get_text().split("on ")[1], "%b %d, %Y at %I:%M %p").strftime("%Y-%m-%d")
                    body = soup.find("div", class_="ds-main").find_all("p")
                    body_text = ""
                    for p in body:
                        body_text += p.get_text() + "\n"
                    topics = [ t.get_text().strip("\n") for t in soup.find("div", class_="gi5--product--summary").find_all("a", attrs={"rel": "bookmark"}) ]
                case "https://www.pcgamer.com/":
                    headline = soup.find("h1").get_text()
                    authors = [ a.get_text() for a in soup.find("div", class_="author-byline__authors").find_all("a", class_="link author-byline__link") ]
                    date = datetime.strptime(soup.find("meta", attrs={"name": "pub_date"}).get("content").split("T")[0], "%Y-%m-%d").strftime("%Y-%m-%d")
                    body = soup.find("div", attrs={"id" : "article-body"}).find_all("p", class_=None)
                    body_text = ""
                    for p in body:
                            body_text += p.get_text() + "\n"
                    topics = [ t.find("a").get_text().strip("\n") for t in soup.find_all("div", class_="tag", attrs={"data-analytics-id" : "article-product"}) ]
        except Exception as e:
            print(f"{self.iden}: Invalid article format. Error: {e}\nSkipping...")
        else:
            self.write_to_elastic_articles(self.iden, headline, date, authors, body_text, topics)


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
                        href.startswith("/gamer-culture/"))
            case "https://www.pcgamer.com/":
                return (href.startswith("https://www.pcgamer.com/games/") or
                        href.startswith("https://www.pcgamer.com/gaming-industry/") or
                        href.startswith("https://www.pcgamer.com/software/"))
            case default: # Stick to chosen sites
                return False

def start_webcrawler(url_base, iden):
    print(f"{iden}: Started at {time.strftime('%X')}")
    WebCrawler(url_base, iden)
    print(f"{iden}: Finished at {time.strftime('%X')}")

def main():
    thread1 = Thread(target=start_webcrawler, args=("https://www.ign.com/", "IGN"))
    thread2 = Thread(target=start_webcrawler, args=("https://www.gameinformer.com/", "GameInformer"))
    thread3 = Thread(target=start_webcrawler, args=("https://www.pcgamer.com/", "PCGamer"))
    thread1.start()
    thread2.start()
    thread3.start()
    thread1.join()
    thread2.join()
    thread3.join()

main()