A Python web scraper designed to collect data from video game news sites to analyze sentiment and trends, the original intent being to find the popularity of video game titles by the number of articles written about them.
Scrapes primary article data into one Elasticsearch index. A second index is used to cache URLs to prevent retreading articles, and a Redis server contains queued links for the web crawler to follow.

The scraper is currently only capable of crawling IGN and PCGamer. Some data from GameInformer was also collected before its recent closure.
