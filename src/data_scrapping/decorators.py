import os
import logging
from bs4 import BeautifulSoup
from src.data_scrapping.data_collector import DataCollector


logger = logging.getLogger(__name__)


class Decorator(DataCollector):
    def __init__(self, collector):
        self._collector = collector
        self.__dict__.update(collector.__dict__)
        super().__init__(
            collector.url_format,
            collector.date2str,
            collector.begin_date,
            collector.end_date,
            collector.timeout,
        )

    def get_all_url(self, archive):
        return self._collector.get_all_url(archive)

    def get_url_content(self, url):
        return self._collector.get_url_content(url)

    def parse_single_page(self, date, url, content_selector):
        return self._collector.parse_single_page(date, url, content_selector)

    def parse_single_section(self, section):
        return self._collector.parse_single_section(section)


class AddPages(Decorator):
    def __init__(self, collector):
        super().__init__(collector)

    def _get_max_page(self, url):
        try:
            content = self._collector.get_url_content(url)
            parsed_content = BeautifulSoup(content, "html.parser")
            selected = parsed_content.select(self._collector.page_selector)
            pages = [
                int(el.text.strip()) for el in selected if el.text.strip().isnumeric()
            ]
            return max(pages) if pages else 1
        except Exception as e:
            logger.debug(e)
            return 0

    def get_all_url(self, archive):
        new_urls = []
        all_urls = self._collector.get_all_url(archive)
        if not hasattr(self._collector, "page_selector"):
            return all_urls

        for date, url in all_urls:
            max_page = self._get_max_page(url)
            for page in range(max_page):
                if page + 1 == 1:
                    new_urls.append((date, url))
                else:
                    new_urls.append(
                        (
                            date,
                            os.path.join(
                                url, self._collector.page_url_suffix.format(page + 1)
                            ),
                        )
                    )

        return new_urls
