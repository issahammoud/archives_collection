import time
import cloudscraper
from abc import ABC, abstractmethod
from src.helpers.enum import headers
from src.utils.logging import logging


logger = logging.getLogger(__name__)


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
