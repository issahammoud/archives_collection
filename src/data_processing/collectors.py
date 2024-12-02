import logging
from functools import partial
from bs4 import BeautifulSoup
from datetime import datetime
from babel.dates import format_datetime
from src.helpers.enum import Archives, DBCOLUMNS
from src.data_processing.data_collector import DataCollector


logger = logging.getLogger(__name__)


class LeMonde(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.lemonde.fr/archives-du-monde/{}/"
        self.archive = Archives.lemonde
        self.content_selector = "section#river > section.teaser"
        self.min_date = datetime.strptime("19-12-1944", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="dd-MM-Y")
        self.has_multiple_pages = False
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def parse_single_section(self, section):
        try:
            figure_url = section.figure.picture.source.get("data-srcset")
            image = self.get_url_content(figure_url)
        except Exception:
            image = None
        title = section.a.h3.text.strip()
        content = section.a.p.text.strip()
        tag = section.a.span.text.strip().title()
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


class LeFigaro(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = (
            "https://recherche.lefigaro.fr/recherche/tout/?datemin={0}&datemax={0}"
        )
        self.archive = Archives.lefigaro
        self.content_selector = "#articles-list > article"
        self.min_date = datetime.strptime("01-01-2005", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="dd-MM-Y")
        self.has_multiple_pages = False

        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def parse_single_section(self, section):
        try:
            figure_url = section.img.get("srcset").split()[-2]
            image = self.get_url_content(figure_url)
        except Exception:
            image = None
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


class LesEchos(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.lesechos.fr/{}"
        self.archive = Archives.lesechos
        self.content_selector = "div > article"
        self.page_selector = "section ul > li > a"
        self.page_url_suffix = "?page={}"
        self.min_date = datetime.strptime("01-01-1991", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="Y/MM")

        self.has_multiple_pages = True
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def parse_single_section(self, section):
        try:
            figure_url = section.a.picture.select("source")[-1].get("srcset")
            image = self.get_url_content(figure_url)
        except Exception:
            image = None
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


class VingthMinutes(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.20minutes.fr/archives/{}"
        self._base_url = "https://www.20minutes.fr"

        self.archive = Archives.vinghtminutes
        self.content_selector = "section > div > ul > li"
        self.min_date = datetime.strptime("01-01-2006", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="Y/MM-dd")
        self.has_multiple_pages = False
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def parse_single_section(self, section):
        section_url = self._base_url + section.a.get("href")
        url_content = self.get_url_content(section_url)
        section_content = BeautifulSoup(url_content, "html.parser")

        try:
            figure_url = section_content.figure.img.get("src")
            image = self.get_url_content(figure_url)
        except Exception:
            image = None
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


class OuestFrance(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.ouest-france.fr/archives/{}"
        self.archive = Archives.ouestfrance
        self.content_selector = "article > div"
        self.page_selector = "nav > ul > li"
        self.page_url_suffix = "?page={}"
        self.min_date = datetime.strptime("01-01-2006", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="Y/dd-MMMM-Y", locale="fr")
        self.has_multiple_pages = True
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def parse_single_section(self, section):
        section_url = section.a.get("href")
        url_content = self.get_url_content(section_url)
        section_content = BeautifulSoup(url_content, "html.parser")

        try:
            figure_url = section_content.article.header.figure.img.get(
                "srcset"
            ).split()[-2]
            image = self.get_url_content(figure_url)
        except Exception:
            image = None
        title = section_content.article.header.h1.text.strip()

        content = section_content.article.header.p.text.strip()
        tag = section_url.split("/")[-2].strip()

        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.link: section_url,
            DBCOLUMNS.archive: self.archive,
        }
        return data


class Liberation(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.liberation.fr/archives/{}"
        self._base_url = "https://www.liberation.fr/"
        self.archive = Archives.liberation
        self.content_selector = "main article"
        self.min_date = datetime.strptime("01-01-1998", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="Y/MM/dd")
        self.has_multiple_pages = False
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def parse_single_section(self, section):
        section_url = self._base_url + section.a.get("href")
        url_content = self.get_url_content(section_url)
        section_content = BeautifulSoup(url_content, "html.parser")

        try:
            figure_url = section_content.main.img.get("src")
            image = self.get_url_content(figure_url)
        except Exception:
            image = None
        title = section_content.main.h1.text.strip()

        content = section_content.select("main > div > div > span")[-1].text.strip()
        tag = section_content.select("main > div > div > div > div > span")[
            -1
        ].text.strip()
        tag = tag if tag else None

        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.link: section_url,
            DBCOLUMNS.archive: self.archive,
        }
        return data


class Mediapart(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.mediapart.fr/journal/une/{}"
        self._base_url = "https://www.mediapart.fr"
        self.archive = Archives.mediapart
        self.content_selector = "section"
        self.min_date = datetime.strptime("01-01-2009", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="ddMMYY")
        self.has_multiple_pages = False
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def parse_single_section(self, section):
        try:
            figure_url = section.img.get("src")
            image = self.get_url_content(figure_url)
        except Exception:
            image = None
        title = section.h2.text.strip()
        content = section.select("p")[-2].text.strip()
        tag = section.select("p")[0].text.strip()
        section_url = self._base_url + section.a.get("href")
        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.link: section_url,
            DBCOLUMNS.archive: self.archive,
        }
        return data


class LeParisien(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.leparisien.fr/archives/{}"
        self._base_url = "https://www.leparisien.fr"
        self.archive = Archives.leparisien
        self.content_selector = "#top div > div > a"
        self.min_date = datetime.strptime("01-04-2009", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="Y/dd-MM-Y")
        self.has_multiple_pages = False
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def parse_single_section(self, section):
        section_url = "https:" + section.get("href")
        logger.debug(f"getting data for section from {section_url}")
        url_content = self.get_url_content(section_url)
        section_content = BeautifulSoup(url_content, "html.parser")

        try:
            figure_url = self._base_url + section_content.section.img.get("src")
            image = self.get_url_content(figure_url)
        except Exception:
            image = None
        title = section_content.h1.text.strip()

        content = section_content.p.text.strip()
        tag = section_url.split("/")[-2]
        tag = tag if tag else None

        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.link: section_url,
            DBCOLUMNS.archive: self.archive,
        }
        return data


class LHumanite(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.humanite.fr/?s=&start_date={0}&end_date={0}"
        self.archive = Archives.lhumanite
        self.content_selector = "article a"
        self.page_selector = "nav > div > div"
        self.page_url_suffix = "&page={}"
        self.min_date = datetime.strptime("01-01-1998", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="Y-MM-dd")
        self.has_multiple_pages = True
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def parse_single_section(self, section):
        section_url = section.get("href")
        logger.debug(f"getting data for section from {section_url}")
        url_content = self.get_url_content(section_url)
        section_content = BeautifulSoup(url_content, "html.parser")

        try:
            figure_url = section_content.figure.img.get("src")
            image = self.get_url_content(figure_url)
        except Exception:
            image = None
        title = section_content.h1.text.strip()

        content = section_content.article.p.text.strip()
        tag = section_content.article.a.text.strip()

        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.link: section_url,
            DBCOLUMNS.archive: self.archive,
        }
        return data
