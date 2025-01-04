import re
import time
import cloudscraper
from selenium import webdriver
from abc import ABC, abstractmethod

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.helpers.enum import headers
from src.utils.logging import logging
from src.helpers.enum import Archives
from src.utils.utils import is_image_url


logger = logging.getLogger(__name__)


class ContentFetchStrategy(ABC):
    @abstractmethod
    def get_url_content(self, url):
        pass


class RequestsFetchStrategy(ContentFetchStrategy):
    def __init__(self, maxsize):
        self._maxsize = maxsize
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
    

class BrowserFetchStrategy(ContentFetchStrategy):
    def __init__(self, has_button):
        self._driver = None
        self.has_button = has_button
        self._cookie_handled = False
        self._count = 0
    
    def _initialize_driver(self):
        if self._driver is None:
            driver_path = "/usr/bin/chromedriver"

            chrome_options = Options()
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--headless")
            self._service = Service(driver_path)
            self._driver = webdriver.Chrome(service=self._service, options=chrome_options)

    def _handle_cookie_consent(self):
        try:
            iframe = WebDriverWait(self._driver, 2).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div#appconsent > iframe"))
        )
            self._driver.switch_to.frame(iframe)
            logger.debug("Switched to iframe")
            consent_button = WebDriverWait(self._driver, 2).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "button__acceptAll"))
            )
            consent_button.click()
            logger.debug("Cookie consent accepted.")
            self._driver.switch_to.default_content()
            self._cookie_handled = True
        except Exception as e:
            logger.debug(f"Cookie consent overlay not found or not interactable: {e}")

    def _click_load_more_button(self):
        try:
            if not self._cookie_handled:
                self._handle_cookie_consent()
            load_more_button = WebDriverWait(self._driver, 2).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "loadmore_btn"))
            )
            load_more_button.click()
            logger.debug(f"Load more button is clicked.")
        except Exception as e:
            logger.debug(f"Load more button not found or not clickable: {e}")

    def _scroll_to_bottom(self):
        self._driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    def _scroll_and_load(self):
        if self.has_button:
            self._click_load_more_button()
        last_height = self._driver.execute_script("return document.body.scrollHeight")
        while True:
            
            self._scroll_to_bottom()

            time.sleep(1)

            new_height = self._driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def get_url_content(self, url):
        self._initialize_driver()
        self._driver.get(url)
        self._scroll_and_load()
        content = self._driver.page_source
        self._count +=1
        self._restart_driver_if_needed()
        return content
    
    def _restart_driver_if_needed(self):
        if self._count % 10 == 0:
            self._driver.quit()
            self._service.stop()
            self._driver = None
            self._count = 0

    def __del__(self):
        if self._driver:
            self._driver.quit()
            self._service.stop()


class StrategyFactory:
    def __init__(self, collector):
        self._collector = collector
        self._request_strategy = RequestsFetchStrategy(maxsize=100)
        self._browser_strategy = BrowserFetchStrategy(collector.archive == Archives.lefigaro)

    def get_url_content(self, url):
        dynamic_page = self._collector.is_dynamic["page"]
        dynamic_section = self._collector.is_dynamic["section"]

        if (not dynamic_page and not dynamic_section) or \
            (not dynamic_page and self._collector.match_format(url)) or \
            is_image_url(url):
            return self._request_strategy.get_url_content(url)
        return self._browser_strategy.get_url_content(url)