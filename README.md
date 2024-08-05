A Python web scraper designed to collect data from video game news sites to analyze sentiment and trends, the original intent being to find the popularity of video game titles by the number of articles written about them.
Scrapes primary article data into one Elasticsearch index. A second index is used to cache URLs to prevent retreading articles, and a Redis server contains queued links for the web crawler to follow.

The scraper is currently only capable of crawling IGN and PCGamer. Some data from GameInformer was also collected before its recent closure.


Kibana Screenshot:
![Kibana Screenshot](https://github.com/user-attachments/assets/9ec0392b-7539-4c0a-bea9-14d2110c9f23)

NOTE: This screenshot was taken after a transfer of all GameInformer articles from a different index and a PCGamer crawl that lasted a few hours. Non-game topics must be manually filtered out.
