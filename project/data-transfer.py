import os
import time
import hashlib
from enum import Enum
from elasticsearch import Elasticsearch, helpers
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Enum for supported sites
SITE = Enum('SITE', 
            [
                ('IGN', "https://www.ign.com/"), 
                ('GameInformer', "https://www.gameinformer.com/"), # RIP GameInformer
                ('PCGamer', "https://www.pcgamer.com/")
                ]
            )

# !! Set ES Cloud ID and API Key Here
ES_CLOUD_ID = os.getenv('ELASTIC_CLOUD_ID')
ES_API_KEY = os.getenv('ELASTIC_API_KEY')

es = Elasticsearch(
            ES_CLOUD_ID,
            api_key=ES_API_KEY
        )
print(str(es.info()) + "\n\n")

new_docs = []
unique_headlines = set()

# Helper function to generate a unique ID for articles
def generate_id(site, headline, date):
    combined_key = "".join(site.split()) + "".join(headline.split()) + "".join(date.split())
    _id = combined_key.encode('utf-8')
    # Elasticsearch did not like using hashed IDs
    return _id
        

def write_to_elastic(es_client, site, headline, date, authors, body, topics):
        print("Writing to Elastic Articles: " + headline)
        es_client.index(
            index='unique-articles', 
            id = generate_id(site, headline, date),
            document={ 
                'site': site, 
                'headline': headline, 
                'date': date, 
                'authors': authors, 
                'body': body, 
                'topics': topics 
            }
        )

def scroll_over_all_docs():
    for hit in helpers.scan(es, index='articles'):
        if (hit['_source']['headline'] not in unique_headlines):
            unique_headlines.add(hit['_source']['headline'])
            new_docs.append(hit)
            print("New Article Found: " + hit['_source']['headline'])

def write_article(article):

    site = article['_source']['site']
    print(site)
    headline = article['_source']['headline']
    print(headline)
    date = article['_source']['date']
    print(date)
    authors = [a for a in article['_source']['author']]
    print(authors)
    body = [b for b in article['_source']['body']]
    print("Body Found")
    topics = [t for t in article['_source']['topics']]
    print(topics)
    write_to_elastic(es, site, headline, date, authors, body, topics)

def main():
    scroll_over_all_docs()
    print("Articles Found: " + str(len(new_docs)))
    for doc in new_docs:
        write_article(doc)

main()

