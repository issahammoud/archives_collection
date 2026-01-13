import logging
import pandas as pd
from sqlalchemy import select, func, true

from app.core.enums import DBCOLUMNS
from app.core.sync_db import SyncDBManager
from app.data_scrapping.data_collector import DataCollector

logger = logging.getLogger(__name__)
db_manager = SyncDBManager()


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

    def get_all_urls(self):
        return self._collector.get_all_urls()

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
        if not hasattr(self._collector, "page_selector"):
            sections, parsed_content = self._collector.get_sections(url.format(page=""))
            return sections, None

        sections, parsed_content = self._collector.get_sections(url.format(page=""))
        max_page = self._get_max_page(parsed_content)
        logger.debug(f"There are {max_page} pages to add")
        for page in range(2, max_page + 1):
            try:
                new_url = url.format(page=self._collector.page_url_suffix.format(page))
                new_sections, _ = self._collector.get_sections(new_url)
                sections += new_sections
            except:
                return sections, None

        return sections, None


class RemoveDoneDates(Decorator):
    def __init__(self, collector):
        super().__init__(collector)
        self._done_dates = self._get_done_dates()
        self._done_urls = None

    def _get_done_dates(self):
        from sqlalchemy import MetaData, Table, inspect

        engine = db_manager.get_engine()
        inspector = inspect(engine)
        if "articles" not in inspector.get_table_names():
            return []

        metadata = MetaData()
        table_ref = Table("articles", metadata, autoload_with=engine)

        with engine.connect() as conn:
            query = select(
                table_ref.c.date.label("date"), func.count().label("freq")
            ).where(
                table_ref.c.archive == self._collector.archive
            ).where(
                table_ref.c.date >= self._collector.begin_date
            ).where(
                table_ref.c.date <= self._collector.end_date
            ).group_by(table_ref.c.date)

            date_counts = query.cte("date_counts")

            median_cte = (
                select(
                    func.percentile_cont(0.5)
                    .within_group(date_counts.c.freq)
                    .label("median_freq")
                )
                .select_from(date_counts)
                .where(date_counts.c.freq > 0)
                .cte("stats")
            )

            final_q = (
                select(date_counts.c.date)
                .select_from(date_counts.join(median_cte, true()))
                .where(date_counts.c.freq > median_cte.c.median_freq * 0.9)
                .order_by(date_counts.c.date)
            )

            result = conn.execute(final_q)
            rows = result.fetchall()
            return [row[0] for row in rows]

    def get_all_urls(self):
        all_urls = super().get_all_urls()
        done_dates = list(zip(self._done_dates, [None] * len(self._done_dates)))

        df = pd.DataFrame(all_urls + done_dates, columns=["date", "str_format"])
        return df.drop_duplicates("date", keep=False).dropna().values[::-1].tolist()

    def _lazy_load_urls(self):
        if self._done_urls is None:
            from sqlalchemy import MetaData, Table, inspect

            engine = db_manager.get_engine()
            inspector = inspect(engine)
            if "articles" not in inspector.get_table_names():
                self._done_urls = set()
                return

            metadata = MetaData()
            table_ref = Table("articles", metadata, autoload_with=engine)

            with engine.connect() as conn:
                query = select(table_ref.c.link).where(
                    table_ref.c.archive == self._collector.archive
                ).where(
                    table_ref.c.date >= self._collector.begin_date
                ).where(
                    table_ref.c.date <= self._collector.end_date
                )

                result = conn.execute(query)
                rows = result.fetchall()
                self._done_urls = set(row[0] for row in rows)
                logger.info(f"{self._collector.archive}: {len(self._done_urls)}")

    def get_section_url(self, section):
        self._lazy_load_urls()
        section_url = super().get_section_url(section)
        return section_url if section_url not in self._done_urls else None
