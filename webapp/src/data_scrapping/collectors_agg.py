import time
import logging
import argparse
import numpy as np
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.helpers.enum import DBCOLUMNS
from src.utils.utils import alternate_elements
from src.helpers.db_connector import DBConnector, DBManager
from src.data_scrapping.collectors_registry import Registry


logger = logging.getLogger(__name__)
db_manager = DBManager()


class CollectorsAggregator:
    def __init__(self, name_list=None, **kwargs) -> None:
        self.collectors = (
            Registry.create_list(name_list, **kwargs)
            if name_list
            else Registry.create_all(**kwargs)
        )
        assert (
            len(self.collectors) > 0
        ), f"Found {len(self.collectors)} collectors. Should have at least 1."
        self.workers = len(self.collectors)

    def get_all_urls(self):
        all_urls = []

        for collector in self.collectors:
            urls = collector.get_all_urls()
            all_urls.append(urls)

        all_urls = alternate_elements(all_urls)
        assert len(all_urls) > 0, "No pages to collect"
        return all_urls

    def parse_single_page(self, args):
        date, url = args
        for collector in self.collectors:
            if collector.match_format(url):
                collector.parse_single_page(date, url)
                break

    def run(self):
        DBConnector.create_table(db_manager.engine, DBConnector.TABLE)
        urls = self.get_all_urls()

        count_before = {}
        for collector in self.collectors:
            name = collector.archive
            count_before[name] = DBConnector.get_total_count(
                db_manager.engine,
                DBConnector.TABLE,
                filters={DBCOLUMNS.archive: [("eq", name)]},
            )
            logger.info(
                f"We already collected {count_before[name]} articles for {name} archive."
            )

        logger.info(f"Getting the data for {len(urls)} dates")
        start = time.time()

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = [executor.submit(self.parse_single_page, url) for url in urls]
            for _ in tqdm(as_completed(futures), total=len(futures), desc="Scraping"):
                pass

        end = np.round((time.time() - start) / 60, 2)

        for collector in self.collectors:
            name = collector.archive
            rows_nb = DBConnector.get_total_count(
                db_manager.engine,
                DBConnector.TABLE,
                filters={DBCOLUMNS.archive: [("eq", name)]},
            )
            diff = rows_nb - count_before[name]
            logger.info(
                f"\nFor {name}:\n"
                f"{diff} sections were collected in {end} min. "
                f"We have in total {rows_nb} sections."
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-s", "--begin_date", type=str, required=True, help="start date"
    )
    parser.add_argument("-e", "--end_date", type=str, required=True, help="end date")
    parser.add_argument("-t", "--timeout", type=float, required=True, help="timeout")
    args = parser.parse_args()

    print(vars(args))
    aggregator = CollectorsAggregator(**vars(args))
    aggregator.run()
