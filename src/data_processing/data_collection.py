import re
import time
import logging
import numpy as np
from tqdm import tqdm
import urllib.request
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count
from src.helpers.db_connector import DBConnector


logger = logging.getLogger(__name__)
engine = DBConnector.get_engine(DBConnector.DBNAME)


class DataCollector:
    URL_FORMAT = "https://www.lemonde.fr/archives-du-monde/{}/"
    FIRST_DATE = "19-12-1944"
    FORMAT = "%d-%m-%Y"
    TIMEOUT = 5
    opener = urllib.request.build_opener()

    @staticmethod
    def get_all_url(from_date=None):
        first_date = from_date if from_date else DataCollector.FIRST_DATE
        first_date = datetime.strptime(first_date, DataCollector.FORMAT)

        current_date = datetime.now()

        all_dates = set()
        for day in range((current_date - first_date).days):
            date = datetime.strftime(
                first_date + timedelta(days=day), DataCollector.FORMAT
            )
            all_dates.add(date)

        done_dates = DBConnector.get_done_dates(engine, DBConnector.TABLE)
        done_dates = set(
            [datetime.strftime(date[0], DataCollector.FORMAT) for date in done_dates]
        )

        logger.info(f"We already collected {len(done_dates)} pages.")

        remaining_dates = all_dates - done_dates
        logger.info(f"There are {len(remaining_dates)} pages to collect.")

        all_urls = [DataCollector.URL_FORMAT.format(date) for date in remaining_dates]

        return all_urls

    @staticmethod
    def parse_single_page(url):
        date = re.findall(DataCollector.URL_FORMAT.format("(.*)"), url)[0]
        date = datetime.strptime(date, DataCollector.FORMAT)

        try:
            req = DataCollector.opener.open(url)
            assert (
                req.getcode() == 200
            ), f"Page {url} not found, error code {req.getcode()}"

            parsed_content = BeautifulSoup(req.read(), "html.parser")
            sections = parsed_content.select("section#river > section.teaser")
            logger.debug(f"Page {url} contains {len(sections)} sections")

            for section in sections:
                try:
                    byte_img, title, content, tag = DataCollector.parse_single_section(
                        section
                    )

                    data = {
                        "date": date,
                        "image": byte_img,
                        "title": title,
                        "content": content,
                        "tag": tag,
                    }

                    DBConnector.insert_row(engine, DBConnector.TABLE, data)
                    logger.debug("Parsed a section successfully")

                except Exception as e:
                    logger.debug(f"Exception in parsing section from page {url}")
                    logger.debug(e)
        except Exception as e:
            logger.debug(f"Exception in parsing page {url}")
            logger.debug(e)

    @staticmethod
    def parse_single_section(section):
        figure_url = section.figure.picture.source.get("data-srcset")
        image = DataCollector.read_image(figure_url)
        title = section.a.h3.text
        content = section.a.p.text
        tag = section.a.span.text
        return image, title, content, tag

    @staticmethod
    def read_image(figure_url):
        if figure_url:
            req = DataCollector.opener.open(figure_url)
            assert (
                req.getcode() == 200
            ), f"Image {figure_url} not found, error code {req.getcode()}"
            return req.read()

    @staticmethod
    def run(from_date=None):
        DBConnector.create_table(engine, DBConnector.TABLE)
        urls = DataCollector.get_all_url(from_date)

        logger.info(f"Getting the data for {len(urls)} dates")
        start = time.time()

        with Pool(cpu_count() // 2) as pool:
            for _ in tqdm(
                pool.imap_unordered(DataCollector.parse_single_page, urls),
                total=len(urls),
            ):
                pass

        end = np.round((time.time() - start) / 60, 2)

        count_done = len(DBConnector.get_done_dates(engine, DBConnector.TABLE))
        rows_nb = DBConnector.get_rows_count(engine, DBConnector.TABLE)

        logger.info(
            f"{count_done} pages were collected in {end} min. "
            f"Among them, we found {rows_nb} sections"
        )
