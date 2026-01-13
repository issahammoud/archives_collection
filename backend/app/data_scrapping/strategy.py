import time
import logging
import cloudscraper
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}


class FetchStrategy(ABC):
    @abstractmethod
    def get_url_content(self, url):
        pass


class RequestsFetchStrategy(FetchStrategy):
    def __init__(self):
        self._start_time = time.time()
        self._init_scraper()

    def _init_scraper(self):
        self.scraper = cloudscraper.create_scraper()

    def _restart_scraper(self):
        if time.time() - self._start_time > 3600:
            self.scraper = None
            self._init_scraper()
            self._start_time = time.time()

    def get_url_content(self, url):
        self._restart_scraper()
        req = self.scraper.get(url, timeout=10, headers=headers)
        assert (
            req.status_code == 200
        ), f"URL {url} not found, error code {req.status_code}"
        return req.content


class StrategyFactory:
    def __init__(self, collector):
        self._collector = collector
        self._request_strategy = RequestsFetchStrategy()

    def get_url_content(self, url):
        return self._request_strategy.get_url_content(url)
