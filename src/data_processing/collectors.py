import logging

from src.helpers.enum import Archives, DBCOLUMNS
from src.data_processing.data_collector import DataCollector


logger = logging.getLogger(__name__)


class LeMondeCollector(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.lemonde.fr/archives-du-monde/{}/"
        date_format = "%d-%m-%Y"
        self.archive = Archives.lemonde
        self.content_selector = "section#river > section.teaser"

        super().__init__(url_format, date_format, begin_date, end_date, timeout)

    def parse_single_section(self, section):
        figure_url = section.figure.picture.source.get("data-srcset")
        image = self.read_image(figure_url)
        title = section.a.h3.text.strip()
        content = section.a.p.text.strip()
        tag = section.a.span.text.strip()
        section_url = section.a.get("href")
        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.link: section_url,
            DBCOLUMNS.archive: self.archive,
        }
        return data


class LeFigaroCollector(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = (
            "https://recherche.lefigaro.fr/recherche/tout/?datemin={0}&datemax={0}"
        )
        date_format = "%d-%m-%Y"
        self.archive = Archives.lefigaro
        self.content_selector = "#articles-list > article"

        super().__init__(url_format, date_format, begin_date, end_date, timeout)

    def parse_single_section(self, section):
        figure_url = section.img.get("srcset").split()[-2]
        image = self.read_image(figure_url)
        title = section.h2.text.strip()
        content = section.div.text.strip()
        tag = section.ul.select("li")[-1].text.strip()
        section_url = section.a.get("href")
        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.link: section_url,
            DBCOLUMNS.archive: self.archive,
        }
        return data
