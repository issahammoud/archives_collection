import re
import logging
import urllib.request
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from src.helpers.enum import DBCOLUMNS
from src.helpers.db_connector import DBConnector


logger = logging.getLogger(__name__)
engine = DBConnector.get_engine(DBConnector.DBNAME)


class DataCollector(ABC):
    def __init__(self, url_format, date_format, begin_date, end_date, timeout):
        super().__init__()
        self.url_format = url_format
        self.date_format = date_format
        self.begin_date = self._convert_date_to_format(begin_date)
        self.end_date = self._convert_date_to_format(end_date)
        self.timeout = timeout
        self.opener = urllib.request.build_opener()
        self.opener.addheaders = [("User-agent", "Mozilla/5.0")]

    def _convert_date_to_format(self, str_date):
        if str_date is not None:
            date = datetime.strptime(str_date, "%d-%m-%Y")
            return datetime.strftime(date, self.date_format)
        return datetime.strftime(datetime.now(), self.date_format)

    def get_all_url(self, archive):
        begin_date = datetime.strptime(self.begin_date, self.date_format)
        end_date = datetime.strptime(self.end_date, self.date_format)

        all_dates = set()
        for day in range((end_date - begin_date + timedelta(days=1)).days):
            date = datetime.strftime(begin_date + timedelta(days=day), self.date_format)
            all_dates.add(date)

        done_dates = DBConnector.get_done_dates(engine, DBConnector.TABLE, archive)
        done_dates = set(
            [datetime.strftime(date[0], self.date_format) for date in done_dates]
        )
        logger.info(f"{archive}: we already collected {len(done_dates)} pages.")

        remaining_dates = all_dates - done_dates
        logger.info(f"{archive}: There are {len(remaining_dates)} pages to collect.")

        all_urls = [self.url_format.format(date) for date in remaining_dates]
        return all_urls

    def get_url_content(self, url):
        req = self.opener.open(url, timeout=self.timeout)
        assert req.getcode() == 200, f"URL {url} not found, error code {req.getcode()}"
        return req.read()

    def parse_single_page(self, url, content_selector):
        pattern = self.url_format.format("(.*)").replace("?", "\?")
        date = re.findall(pattern, url)[0]
        date = date[0] if isinstance(date, tuple) else date
        date = datetime.strptime(date, self.date_format)

        try:
            content = self.get_url_content(url)
            parsed_content = BeautifulSoup(content, "html.parser")
            sections = parsed_content.select(content_selector)
            logger.debug(f"Page {url} contains {len(sections)} sections")

            for section in sections:
                try:
                    data = self.parse_single_section(section)

                    data[DBCOLUMNS.date] = date

                    DBConnector.insert_row(engine, DBConnector.TABLE, data)
                    logger.debug("Parsed a section successfully")

                except Exception as e:
                    logger.debug(f"Exception in parsing section from page {url}")
                    logger.debug(e)
        except Exception as e:
            logger.debug(f"Exception in parsing page {url}")
            logger.debug(e)

    @abstractmethod
    def parse_single_section(self, section):
        raise NotImplementedError
