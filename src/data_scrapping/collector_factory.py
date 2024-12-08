import re
import time
import logging
import numpy as np
from tqdm import tqdm
from multiprocessing import Pool

from src.helpers.enum import Archives
from src.helpers.db_connector import DBConnector

from src.utils.utils import alternate_elements
from src.data_scrapping.decorators import AddPages
from src.data_scrapping.collectors import (
    LeMonde,
    LeFigaro,
    LesEchos,
    VingthMinutes,
    OuestFrance,
    Liberation,
    Mediapart,
    LeParisien,
    LHumanite,
)


logger = logging.getLogger(__name__)
engine = DBConnector.get_engine(DBConnector.DBNAME)


class CollectorFactory:
    MAPPING = {
        Archives.lemonde: LeMonde,
        Archives.lefigaro: LeFigaro,
        Archives.lesechos: LesEchos,
        Archives.vinghtminutes: VingthMinutes,
        Archives.ouestfrance: OuestFrance,
        Archives.liberation: Liberation,
        Archives.mediapart: Mediapart,
        Archives.leparisien: LeParisien,
        Archives.lhumanite: LHumanite,
    }

    def __init__(self, collectors_names, workers, **kwargs) -> None:
        self.collectors = []
        self.workers = workers
        self.collectors_names = collectors_names
        for name in collectors_names:
            assert (
                name in CollectorFactory.MAPPING
            ), f"Unknown collector {name}. Should be one of {Archives}"
            collector = CollectorFactory.MAPPING[name](**kwargs)
            if collector.has_multiple_pages:
                collector = AddPages(collector)
            self.collectors.append(collector)

        assert (
            len(self.collectors) > 0
        ), f"Found {len(self.collectors)} collectors. Should have at least 1."

    def get_all_url(self):
        all_urls = []

        for collector in self.collectors:
            all_urls.append(collector.get_all_url(collector.archive))

        all_urls = alternate_elements(all_urls)
        assert len(all_urls) > 0, "No pages to collect"
        return all_urls

    def parse_single_page(self, args):
        date, url = args
        for collector in self.collectors:
            if re.match(collector.url_format.replace("?", "\?").format(".*"), url):
                collector.parse_single_page(date, url)
                break

    def run(self):
        DBConnector.create_table(engine, DBConnector.TABLE)
        urls = self.get_all_url()

        count_before = {}
        for name in self.collectors_names:
            count_before[name] = DBConnector.get_archive_count(
                engine, DBConnector.TABLE, name
            )

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
            rows_nb = DBConnector.get_archive_count(engine, DBConnector.TABLE, name)
            diff = rows_nb - count_before[name]
            logger.info(
                f"\nFor {name}:\n"
                f"{diff} sections were collected in {end} min. "
                f"We have in total {rows_nb} sections."
            )
