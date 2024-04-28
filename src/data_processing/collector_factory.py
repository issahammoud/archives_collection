import re
import time
import logging
import numpy as np
from tqdm import tqdm
from multiprocessing import Pool

from src.helpers.enum import Archives
from src.helpers.db_connector import DBConnector

from src.utils.utils import alternate_elements
from src.data_processing.collectors import LeMondeCollector, LeFigaroCollector


logger = logging.getLogger(__name__)
engine = DBConnector.get_engine(DBConnector.DBNAME)


class CollectorFactory:
    MAPPING = {Archives.lemonde: LeMondeCollector, Archives.lefigaro: LeFigaroCollector}

    def __init__(self, collectors_names, workers, **kwargs) -> None:
        self.collectors = []
        self.workers = workers
        self.collectors_names = collectors_names
        for name in collectors_names:
            assert (
                name in CollectorFactory.MAPPING
            ), f"Unknown collector {name}. Should be one of {Archives}"
            self.collectors.append(CollectorFactory.MAPPING[name](**kwargs))

        assert (
            len(self.collectors) > 0
        ), f"Found {len(self.collectors)} collectors. Should have at least 1."

    def get_all_url(self):
        all_urls = []

        for collector in self.collectors:
            all_urls.append(collector.get_all_url(collector.archive))

        return alternate_elements(all_urls)

    def parse_single_page(self, url):
        for collector in self.collectors:
            if re.match(collector.url_format.replace("?", "\?").format(".*"), url):
                collector.parse_single_page(url, collector.content_selector)
                break

    def run(self):
        DBConnector.create_table(engine, DBConnector.TABLE)
        urls = self.get_all_url()

        logger.info(f"Getting the data for {len(urls)} dates")
        start = time.time()

        with Pool(self.workers) as pool:
            for _ in tqdm(
                pool.imap_unordered(self.parse_single_page, urls),
                total=len(urls),
            ):
                pass

        end = np.round((time.time() - start) / 60, 2)

        for name in self.collectors_names:
            count_done = len(
                DBConnector.get_done_dates(engine, DBConnector.TABLE, name)
            )
            rows_nb = DBConnector.get_count(engine, DBConnector.TABLE, name)

            logger.info(
                f"\nFor {name}:\n"
                f"{count_done} pages were collected in {end} min. "
                f"Among them, we found {rows_nb} sections"
            )
