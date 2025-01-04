import os
import logging

from src.utils.utils import hash_url
from src.helpers.enum import DBCOLUMNS
from src.helpers.db_connector import DBConnector
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

    def match_format(self, url):
        return self._collector.match_format(url)

    def get_all_url(self):
        return self._collector.get_all_url()

    def get_url_content(self, url):
        return self._collector.get_url_content(url)

    def get_sections(self, url):
        return self._collector.get_sections(url)

    def parse_single_page(self, date, url):
        return self._collector.parse_single_page(date, url)

    def parse_single_section(self, section, section_url):
        return self._collector.parse_single_section(section, section_url)

    def get_section_url(self, section):
        return self._collector.get_section_url(section)


class AddPages(Decorator):
    def __init__(self, collector):
        super().__init__(collector)

    def _get_max_page(self, parsed_content):
        try:
            selected = parsed_content.select(self._collector.page_selector)
            pages = [
                int(el.text.strip()) for el in selected if el.text.strip().isnumeric()
            ]
            return max(pages) if pages else 1
        except Exception as e:
            logger.debug(e)
            return 0

    def get_sections(self, url):
        sections, parsed_content = self._collector.get_sections(url)
        if not hasattr(self._collector, "page_selector"):
            return sections, None

        max_page = self._get_max_page(parsed_content)
        logger.debug(f"There are {max_page} pages to add")
        for page in range(2, max_page + 1):
            try:
                new_url = os.path.join(
                    url, self._collector.page_url_suffix.format(page)
                )
                new_sections, _ = self._collector.get_sections(new_url)
                sections += new_sections
            except:
                return sections, None

        return sections, None


class EliminateRedundancy(Decorator):
    def __init__(self, collector):
        super().__init__(collector)
        self._done = None

    def _lazy_load_rowids(self):
        if self._done is None:
            self._done = set(
                DBConnector.get_all_rowid(
                    self.engine,
                    DBConnector.TABLE,
                    filters={DBCOLUMNS.archive: [("eq", self._collector.archive)]},
                )
            )

    def get_section_url(self, section):
        self._lazy_load_rowids()
        url = self._collector.get_section_url(section)
        return None if hash_url(url) in self._done else url
