import re
import os
import logging
import dateparser
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, date

from src.helpers.enum import DBCOLUMNS
from src.helpers.db_connector import DBConnector
from src.data_scrapping.strategy import StrategyFactory
from src.utils.utils import save_image, get_image_path, get_embeddings

logger = logging.getLogger(__name__)


class DataCollector(ABC):

    def __init__(self, url_format, date2str, begin_date, end_date, timeout):
        super().__init__()
        self.url_format = url_format
        self.date2str = date2str
        self.begin_date = self._convert_to_date(begin_date)
        self.begin_date = max(self.begin_date, self.min_date)
        self.end_date = self._convert_to_date(end_date)
        self.timeout = timeout
        self._translation_table = str.maketrans("éàèùâêîôûç", "eaeuaeiouc")
        self._fetch_strategy = StrategyFactory(self)
        self.engine = None
        self._data_dir = "/images/"
        self._embedding_url = os.getenv("EMBED_URL")

    def _init_engine(self):
        if self.engine is None:
            self.engine = DBConnector.get_engine()

    def match_format(self, url):
        return bool(
            re.match(self.url_format.replace("?", "\?").format(date=".*", page=""), url)
        )

    def _convert_to_date(self, str_date):
        if str_date is not None:
            if isinstance(str_date, date):
                return str_date
            return dateparser.parse(str_date, ["%d-%m-%Y"]).date()
        return datetime.now().date()

    def get_all_urls(self):
        all_dates = []
        for day in range((self.end_date - self.begin_date + timedelta(days=1)).days):
            date = self.begin_date + timedelta(days=day)
            all_dates.append(date)

        all_urls = [
            (
                date,
                self.url_format.format(
                    date=self.date2str(date).translate(self._translation_table),
                    page="{page}",
                ),
            )
            for date in all_dates
        ]
        df = pd.DataFrame(all_urls, columns=["date", "str_format"])
        return df.drop_duplicates("str_format").values[::-1].tolist()

    def get_url_content(self, url):
        return self._fetch_strategy.get_url_content(url)

    def get_sections(self, url):
        content = self.get_url_content(url.format(page=""))
        parsed_content = BeautifulSoup(content, "html.parser")
        sections = parsed_content.select(self.content_selector)
        return sections, parsed_content

    def parse_single_page(self, date, url):
        self._init_engine()
        try:
            sections, _ = self.get_sections(url)
            logger.debug(f"Page {url} contains {len(sections)} sections")
            data_list = []
            for section in sections:
                try:
                    section_url = self.get_section_url(section)
                    if section_url is not None:
                        data = self.parse_single_section(section, section_url)

                        data[DBCOLUMNS.date] = date
                        data[DBCOLUMNS.link] = section_url
                        img_path = get_image_path(self._data_dir, date, section_url)
                        img_path = save_image(img_path, data[DBCOLUMNS.image])
                        data[DBCOLUMNS.image] = img_path
                        data_list.append(data)

                        if len(data_list) >= 32:
                            self.insert_batch(data_list)
                            data_list = []

                except Exception as e:
                    logger.debug(f"Exception in parsing section from page {url}")
                    logger.debug(e)
                    if len(data_list) >= 32:
                        self.insert_batch(data_list)
                        data_list = []

            if len(data_list) >= 32:
                self.insert_batch(data_list)
                data_list = []

        except Exception as e:
            logger.debug(f"Exception in parsing page {url}")
            logger.debug(e)

    def insert_batch(self, data_list):
        list_ = []
        embeddings = get_embeddings(data_list, self._embedding_url)
        for data, emb in zip(data_list, embeddings):
            data[DBCOLUMNS.embedding] = emb
            list_.append(data)

        rowscount = DBConnector.insert_row(self.engine, DBConnector.TABLE, list_)
        logger.info(f"{rowscount} were inserted into the database")

    @abstractmethod
    def get_section_url(self, section):
        raise NotImplementedError

    @abstractmethod
    def parse_single_section(self, section, section_url):
        raise NotImplementedError
