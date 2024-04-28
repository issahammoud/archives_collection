import logging
from bs4 import BeautifulSoup
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
        image = self.get_url_content(figure_url)
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
        image = self.get_url_content(figure_url)
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


class LesEchosCollector(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.lesechos.fr/{}"
        date_format = "%Y/%m"
        self.archive = Archives.lesechos
        self.content_selector = "div > article"

        super().__init__(url_format, date_format, begin_date, end_date, timeout)

    def parse_single_section(self, section):
        figure_url = section.a.picture.select("source")[-1].get("srcset")
        image = self.get_url_content(figure_url)
        title = section.h3.text.strip()
        content = section.select("a")[1].select("div")[-1].text.strip()
        tag = None
        section_url = section.a.get("href").strip()
        section_url = section_url[1:] if section_url[0] == "/" else section_url
        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.link: self.url_format.format(section_url),
            DBCOLUMNS.archive: self.archive,
        }
        return data


class VingthMinutesCollector(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.20minutes.fr/archives/{}"
        self._base_url = "https://www.20minutes.fr"
        date_format = "%Y/%m-%d"
        self.archive = Archives.vinghtminutes
        self.content_selector = "section > div > ul > li"
        super().__init__(url_format, date_format, begin_date, end_date, timeout)

    def parse_single_section(self, section):
        section_url = self._base_url + section.a.get("href")
        url_content = self.get_url_content(section_url)
        section_content = BeautifulSoup(url_content, "html.parser")

        figure_url = section_content.figure.img.get("src")
        image = self.get_url_content(figure_url)
        title = section_content.h1.text.strip()

        content = section_content.select("header > div > span")[-1].text.strip()
        tag = section_content.select("header > div > span")[0].text.strip()

        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.link: section_url,
            DBCOLUMNS.archive: self.archive,
        }
        return data
