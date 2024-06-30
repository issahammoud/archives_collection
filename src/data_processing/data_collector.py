import logging
import requests
import dateparser
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, date

from src.helpers.enum import DBCOLUMNS, headers
from src.helpers.db_connector import DBConnector


logger = logging.getLogger(__name__)
engine = DBConnector.get_engine(DBConnector.DBNAME)


class DataCollector(ABC):
    def __init__(self, url_format, date2str, begin_date, end_date, timeout):
        super().__init__()
        self.url_format = url_format
        self.date2str = date2str
        self.begin_date = self._convert_to_date(begin_date)
        self.begin_date = max(self.begin_date, self.min_date)
        self.end_date = self._convert_to_date(end_date)
        self.timeout = timeout

    def _convert_to_date(self, str_date):
        if str_date is not None:
            if isinstance(str_date, date):
                return str_date
            return dateparser.parse(str_date, ["%d-%m-%Y"]).date()
        return datetime.now().date()

    def get_all_url(self, archive):
        all_dates = set()
        for day in range((self.end_date - self.begin_date + timedelta(days=1)).days):
            date = self.begin_date + timedelta(days=day)
            all_dates.add(date)

        done_dates = DBConnector.get_done_dates(engine, DBConnector.TABLE, archive)
        done_dates = set([date[0].date() for date in done_dates])
        logger.info(f"{archive}: we already collected {len(done_dates)} pages.")

        remaining_dates = all_dates - done_dates
        logger.info(f"{archive}: There are {len(remaining_dates)} pages to collect.")

        remaining_dates = sorted(remaining_dates)[::-1]
        all_urls = [
            (date, self.url_format.format(self.date2str(date)))
            for date in remaining_dates
        ]
        return all_urls

    def get_url_content(self, url):
        req = requests.get(url, timeout=self.timeout, headers=headers)
        assert (
            req.status_code == 200
        ), f"URL {url} not found, error code {req.status_code}"
        return req.content

    def parse_single_page(self, date, url, content_selector):
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
