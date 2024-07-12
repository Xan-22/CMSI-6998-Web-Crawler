##########################################################
#           Second Iteration based on sample code        #
#           Neo4J Implementation commented               #
##########################################################

import mechanicalsoup as ms
import redis
import os
from elasticsearch import Elasticsearch
# from neo4j import GraphDatabase

# class Neo4JConnector:
#     def __init__(self, uri, user, password):
#         self.driver = GraphDatabase.driver(uri, auth=(user, password))

#     def close(self):
#         self.driver.close()

#     def add_links(self, page, links):
#         with self.driver.session() as session: 
#             session.execute_write(self._create_links, page, links)

#     def flush_db(self):
#         print("clearing graph db")
#         with self.driver.session() as session: 
#             session.execute_write(self._flush_db)
    
#     @staticmethod
#     def _create_links(tx, page, links):
#         # because Redis stores strings as ByteStrings,
#         # we must decode our url into a string of a specific encoding
#         # for it to be valid JSON
#         page = page.decode('utf-8')
#         tx.run("CREATE (:Page { url: $page })", page=page)
#         for link in links:
#             tx.run("MATCH (p:Page) WHERE p.url = $page "
#                    "CREATE (:Page { url: $link }) -[:LINKS_TO]-> (p)",
#                    link=link, page=page)

#     @staticmethod
#     def _flush_db(tx):
#         tx.run("MATCH (a) -[r]-> () DELETE a, r")
#         tx.run("MATCH (a) DELETE a")


class WebCrawler:
    def __init__(self, starting_url, target_url):
        # # Initialize Neo4j
        # self.neo = Neo4JConnector("bolt://localhost:7687", "", "")
        # self.neo.flush_db()

        # Initialize Elasticsearch
        username = 'elastic'
        password = os.getenv('ELASTIC_PASSWORD') # should be 'elastic' for now. make sure it's in the terminal environment.

        self.es_client = Elasticsearch(
           "http://localhost:9200",
           basic_auth=(username, password)
        )

        print(self.es_client.info())

       # Initialize Redis
        self.r = redis.Redis()
        self.r.flushall()

       # Initialize MechanicalSoup headless browser
        self.browser = ms.StatefulBrowser()
        self.run(starting_url, target_url)

        # # Close connection to Neo4j
        # self.neo.close()

    def run(self, starting_url, target_url):
        # Add root url as the entrypoint to our crawl
        self.r.lpush("links", starting_url)
        # Start crawl
        while link := self.r.rpop("links"):
            if (link == target_url):
                print('Target URL found!')
                break
            self.crawl(link)

    def write_to_elastic(self, decoded_url, html):
        # Keeping this function separate for readability
        self.es_client.index(index='webpages', document={ 'url': decoded_url, 'html': html })

    def crawl(self, url):
        # Download url

        # because Redis stores strings as ByteStrings,
        # we must decode our url into a string of a specific encoding
        # for it to be valid JSON
        # We will keep the decoded url here to prevent repetitive decoding
        decoded_url = url.decode('utf-8')

        print(decoded_url.split('/')[-1])
        self.browser.open(url)

        # Cache page to elasticsearch
        self.write_to_elastic(decoded_url, str(self.browser.page))

        # Parse for more urls
        a_tags = self.browser.page.find_all("a")
        hrefs = [ a.get("href") for a in a_tags ]

        # Do wikipedia specific URL filtering
        wikipedia_domain = "https://en.wikipedia.org"
        links = [ wikipedia_domain + a for a in hrefs if a and a.startswith("/wiki/") ]

        # Put urls in Redis queue
        # create a linked list in Redis, call it "links"
        self.r.lpush("links", *links)

        # # Add links to Neo4J graph
        # self.neo.add_links(url, links)


WebCrawler("https://en.wikipedia.org/wiki/Redis", "https://en.wikipedia.org/wiki/Jesus")
