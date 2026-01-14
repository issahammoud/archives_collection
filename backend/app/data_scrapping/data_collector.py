import re
import os
import logging
import hashlib
import dateparser
import pandas as pd
import requests
import orjson
import numpy as np
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, date
from wand.image import Image as WandImage

from app.core.config import settings
from app.core.enums import DBCOLUMNS
from app.core.sync_db import SyncDBManager
from app.data_scrapping.strategy import StrategyFactory

logger = logging.getLogger(__name__)
db_manager = SyncDBManager()


def save_image(file_path, image_bytes, quality=80):
    if image_bytes is not None:
        try:
            with WandImage(blob=image_bytes) as img:
                img.quality = quality
                img.format = "webp"
                img.save(filename=file_path)
        except Exception:
            with open(file_path, "wb") as file:
                file.write(image_bytes)
        finally:
            return file_path
    return None


def get_image_path(data_dir, date, section_url):
    hash_url = hashlib.sha256(section_url.encode("utf-8")).hexdigest()
    try:
        year = date.year
        month = date.month
    except Exception:
        year = "unknown"
        month = "unknown"

    subdir = os.path.join(data_dir, str(year), str(month))
    os.makedirs(subdir, exist_ok=True)
    file_name = f"{hash_url}.webp"
    file_path = os.path.join(subdir, file_name)
    return file_path


def get_embeddings(batch, embedding_url, timeout=20):
    data = []
    for el in batch:
        text = ""
        text += (
            f"title: {el.get(DBCOLUMNS.title, '')}\n" if el.get(DBCOLUMNS.title) else ""
        )
        text += (
            f"content: {el.get(DBCOLUMNS.content, '')}\n"
            if el.get(DBCOLUMNS.content)
            else ""
        )
        text += f"topic: {el.get(DBCOLUMNS.tag, '')}" if el.get(DBCOLUMNS.tag) else ""
        data.append(text)

    payload = {"data": data}

    try:
        resp = requests.post(embedding_url, json=payload, timeout=timeout)
        resp.raise_for_status()
        data = orjson.loads(resp.content)
        embeddings = np.array(data["embeddings"])
        if np.any(embeddings == None):
            return None
        return embeddings

    except Exception as e:
        logger.error(f"Error fetching embeddings for batch of size {len(batch)}: {e}")
        return None


class DataCollector(ABC):
    BATCH_EMBEDDING = 32

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
        self._data_dir = "/images/"
        self._embedding_url = os.getenv("EMBED_URL")

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

                        if len(data_list) >= DataCollector.BATCH_EMBEDDING:
                            self.insert_batch(data_list)
                            data_list = []

                except Exception as e:
                    logger.debug(f"Exception in parsing section from page {url}")
                    logger.debug(e)
                    if len(data_list) >= DataCollector.BATCH_EMBEDDING:
                        self.insert_batch(data_list)
                        data_list = []

            if len(data_list) > 0:
                self.insert_batch(data_list)
                data_list = []

        except Exception as e:
            logger.debug(f"Exception in parsing page {url}")
            logger.debug(e)

    def insert_batch(self, data_list):
        list_ = []
        embeddings = get_embeddings(data_list, self._embedding_url)
        if embeddings is not None:
            for data, emb in zip(data_list, embeddings):
                data[DBCOLUMNS.embedding] = (
                    emb.tolist() if hasattr(emb, "tolist") else emb
                )
                list_.append(data)

            rowscount = db_manager.insert_rows("articles", list_)
        else:
            rowscount = db_manager.insert_rows("articles", data_list)

        logger.info(f"{rowscount} were inserted into the database")

    @abstractmethod
    def get_section_url(self, section):
        raise NotImplementedError

    @abstractmethod
    def parse_single_section(self, section, section_url):
        raise NotImplementedError
