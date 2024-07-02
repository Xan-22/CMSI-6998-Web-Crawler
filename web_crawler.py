import requests
from bs4 import BeautifulSoup

class WebCrawler: #Implements BFS
    def __init__(self, starting_url, target_url):
        self.target_url = target_url
        self.current_url = starting_url
        self.queue = [starting_url]
        self.visited = set()
        self.run()

    def get_all_links(self, url):
        try:
            response = requests.get("https://en.wikipedia.org" + url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                return soup.find_all('a')
        except Exception:
            return []
    
    def run(self):
        while self.queue != []:
             if (self.current_url == self.target_url):
                 print('Target URL found!')
                 return
             self.current_url = self.queue.pop(0)
             self.visited.add(self.current_url)
             print (self.current_url.split('/')[-1])
             self.crawl(self.current_url)
        print('Target URL not found')

    def crawl(self, url):
        if (url is None) or (url == ''):
            return
        links = self.get_all_links(url)
        if (links is not None):
            for link in links:
                ref = link.get('href')
                if ref is not None and ref not in self.queue and ref not in self.visited and ref.startswith('/wiki/'):
                    self.queue.append(ref)
                
#"5-Clicks-to-Jesus game" used as an example (https://en.wikipedia.org/wiki/Wikipedia:Wiki_Game)
c = WebCrawler('/wiki/Star_Wars', "/wiki/Pokémon")