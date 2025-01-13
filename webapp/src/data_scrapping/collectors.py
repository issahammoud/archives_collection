import re
import logging
from functools import partial
from bs4 import BeautifulSoup
from datetime import datetime
from babel.dates import format_datetime
from src.helpers.enum import Archives, DBCOLUMNS
from src.data_scrapping.data_collector import DataCollector
from src.data_scrapping.collectors_registry import Registry


logger = logging.getLogger(__name__)

@Registry.register()
class LeMonde(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.lemonde.fr/archives-du-monde/{date}/{page}"
        self.archive = Archives.lemonde
        self.content_selector = "section#river > section.teaser"
        self.min_date = datetime.strptime("19-12-1944", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="dd-MM-y")
        self.page_selector = "section.river__pagination > a"
        self.page_url_suffix = "{}/"
        self.is_dynamic = {"page": False, "section": False}
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def get_section_url(self, section):
        return section.a.get("href")

    def parse_single_section(self, section, section_url):
        try:
            figure_url = section.figure.picture.source.get("data-srcset")
            image = self.get_url_content(figure_url)
        except Exception:
            image = None
        title = section.h3.text.strip()
        content = section.p.text.strip()
        try:
            tag = section.span.a.text.strip()
        except:
            tag = None
        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.archive: self.archive,
        }
        return data


class LeFigaro(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = (
            "https://recherche.lefigaro.fr/recherche/_/?datemin={date}&datemax={date}"
        )
        self.archive = Archives.lefigaro
        self.content_selector = "#articles-list > article"
        self.min_date = datetime.strptime("01-01-2005", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="dd-MM-y")
        self.is_dynamic = {"page": True, "section": False}
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def get_section_url(self, section):
        return section.a.get("href")

    def parse_single_section(self, section, section_url):
        try:
            figure_url = section.img.get("srcset").split()[-2]
            image = self.get_url_content(figure_url)
        except Exception:
            image = None
        title = section.h2.text.strip()
        content = section.select("div")[-1].text.strip()
        try:
            tag = section.ul.select("li")[-1].text.strip()
        except:
            tag = None
        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.archive: self.archive,
        }
        return data


@Registry.register()
class LesEchos(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.lesechos.fr/{date}{page}"
        self.archive = Archives.lesechos
        self.content_selector = "div > article"
        self.page_selector = "section ul > li > a"
        self.page_url_suffix = "?page={}"
        self.min_date = datetime.strptime("01-01-1991", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="y/MM")
        self.is_dynamic = {"page": False, "section": False}
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def get_section_url(self, section):
        section_url = section.a.get("href").strip()
        section_url = section_url[1:] if section_url[0] == "/" else section_url
        return self.url_format.format(section_url)

    def parse_single_section(self, section, section_url):
        try:
            figure_url = section.a.picture.img.get("src")
            image = self.get_url_content(figure_url)
        except Exception:
            image = None
        title = section.h3.text.strip()
        content = section.a.select("div")[-1].text.strip()
        tag = None
        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.archive: self.archive,
        }
        return data


@Registry.register()
class VingthMinutes(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.20minutes.fr/archives/{date}"
        self._base_url = "https://www.20minutes.fr"

        self.archive = Archives.vinghtminutes
        self.content_selector = (
            "article > div > div > div > div.grid > div.c-bulleted-list__item"
        )
        self.min_date = datetime.strptime("01-01-2006", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="y/MM-dd")
        self.is_dynamic = {"page": False, "section": False}

        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def get_section_url(self, section):
        return section.a.get("href")

    def parse_single_section(self, section, section_url):
        url_content = self.get_url_content(section_url)
        section_content = BeautifulSoup(url_content, "html.parser")

        try:
            figure_url = section_content.figure.img.get("src")
            image = self.get_url_content(figure_url)
        except Exception:
            image = None
        title = section_content.h1.text.strip()

        content = section_content.select("header > div > span")[-1].text.strip()
        try:
            tag = section_content.select("header > div > span")[0].text.strip()
        except:
            tag = None
        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.archive: self.archive,
        }
        return data


@Registry.register()
class OuestFrance(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.ouest-france.fr/archives/{date}/{page}"
        self.archive = Archives.ouestfrance
        self.content_selector = "article > div"
        self.page_selector = "nav > ul > li"
        self.page_url_suffix = "?page={}"
        self.min_date = datetime.strptime("01-01-2012", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="y/dd-MMMM-y", locale="fr")
        self.is_dynamic = {"page": False, "section": False}
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def get_section_url(self, section):
        return section.a.get("href")

    def parse_single_section(self, section, section_url):
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
        try:
            tag = section_url.split("/")[-2].strip()
        except:
            tag = None
        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.archive: self.archive,
        }
        return data


class Liberation(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.liberation.fr/archives/{date}"
        self._base_url = "https://www.liberation.fr"
        self.archive = Archives.liberation
        self.content_selector = "main article"
        self.min_date = datetime.strptime("01-01-1998", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="y/MM/dd")
        self.is_dynamic = {"page": False, "section": True}
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def get_section_url(self, section):
        return self._base_url + section.a.get("href")

    def parse_single_section(self, section, section_url):
        url_content = self.get_url_content(section_url)
        section_content = BeautifulSoup(url_content, "html.parser")

        try:
            figure_url = section_content.main.figure.img.get("src")
            image = self.get_url_content(figure_url)
        except Exception:
            image = None
        title = section_content.main.h1.text.strip()

        content = " ".join(
            [
                span.text.strip()
                for span in section_content.select("main > div > div > span")
                if len(span.text) > 5
            ]
        ).strip()
        try:
            tag = section_content.select("main > div > div > div > div > span")[
                0
            ].text.strip()
        except:
            tag = None

        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.archive: self.archive,
        }
        return data


@Registry.register()
class Mediapart(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.mediapart.fr/journal/une/{date}"
        self._base_url = "https://www.mediapart.fr"
        self.archive = Archives.mediapart
        self.content_selector = "h3"
        self.min_date = datetime.strptime("01-01-2009", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="ddMMyy")
        self.is_dynamic = {"page": False, "section": False}
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def get_section_url(self, section):
        return self._base_url + section.a.get("href")

    def parse_single_section(self, section, section_url):
        url_content = self.get_url_content(section_url)
        section_content = BeautifulSoup(url_content, "html.parser")

        try:
            figure_url = section.img.get("src")
            image = self.get_url_content(figure_url)
        except Exception:
            image = None
        title = section_content.h1.text.strip()
        content = section_content.main.select("p")[1].text.strip()
        try:
            tag = section_content.main.select("p")[0].text.strip()
        except:
            tag = None
        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.archive: self.archive,
        }
        return data


@Registry.register()
class LeParisien(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.leparisien.fr/archives/{date}"
        self._base_url = "https://www.leparisien.fr"
        self.archive = Archives.leparisien
        self.content_selector = "#top div > div > a"
        self.min_date = datetime.strptime("01-04-2009", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="y/dd-MM-y")
        self.is_dynamic = {"page": False, "section": False}
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def get_section_url(self, section):
        return "https:" + section.get("href")

    def parse_single_section(self, section, section_url):
        url_content = self.get_url_content(section_url)
        section_content = BeautifulSoup(url_content, "html.parser")

        try:
            figure_url = self._base_url + section_content.section.img.get("src")
            image = self.get_url_content(figure_url)
        except Exception:
            image = None
        title = section_content.h1.text.strip()

        content = section_content.p.text.strip()
        try:
            tag = section_url.split("/")[-2]
        except:
            tag = None

        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.archive: self.archive,
        }
        return data


@Registry.register()
class LHumanite(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.humanite.fr/toutes-les-archives{page}?{date}"
        self.archive = Archives.lhumanite
        self.content_selector = "article a"
        self.page_selector = "nav > div > div a"
        self.page_url_suffix = "/page/{}"
        self.min_date = datetime.strptime("01-01-1998", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="'jour='d'&mois='M'&annee='y")
        self.is_dynamic = {"page": False, "section": False}
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def get_section_url(self, section):
        return section.get("href")

    def parse_single_section(self, section, section_url):
        logger.debug(f"getting data for section from {section_url}")
        url_content = self.get_url_content(section_url)
        section_content = BeautifulSoup(url_content, "html.parser")

        try:
            figure_url = section_content.figure.img.get("src")
            image = self.get_url_content(figure_url)
        except Exception:
            image = None
        title = section_content.h1.text.strip()

        content = "\n".join(
            [p.text.strip() for p in section_content.article.select("p")]
        )
        try:
            tag = section_content.article.select("a.rubric")[0].text.strip()
        except:
            tag = None

        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.archive: self.archive,
        }
        return data


@Registry.register()
class LePoint(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.lepoint.fr/archives/{date}.php"
        self._base_url = "https://www.lepoint.fr"

        self.archive = Archives.lepoint
        self.content_selector = "main > article"
        self.min_date = datetime.strptime("01-05-2010", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="MM-y/dd")
        self.is_dynamic = {"page": False, "section": False}
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def get_section_url(self, section):
        return self._base_url + section.a.get("href")

    def parse_single_section(self, section, section_url):
        url_content = self.get_url_content(section_url)
        section_content = BeautifulSoup(url_content, "html.parser")

        try:
            figure_url = section_content.figure.img.get("src")
            image = self.get_url_content(figure_url)
        except Exception:
            image = None
        title = section_content.h1.text.strip()

        content = section_content.select("div#contenu")[0].text.strip()

        try:
            tag = section_content.select("main > ul > li")[0].text.strip()
        except:
            tag = None
        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.archive: self.archive,
        }
        return data


@Registry.register()
class LOrient(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.lorientlejour.com/seo.php?date={date}"
        self._base_url = "https://www.lorientlejour.com"

        self.archive = Archives.lorient
        self.content_selector = "div.articles > ul > li"
        self.min_date = datetime.strptime("01-01-1997", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="y-MM-dd")
        self.is_dynamic = {"page": False, "section": False}
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def get_section_url(self, section):
        return self._base_url + section.a.get("href")

    def parse_single_section(self, section, section_url):
        url_content = self.get_url_content(section_url)
        section_content = BeautifulSoup(url_content, "html.parser")

        try:
            figure_url = section_content.select("div.image-container img")[0].get("src")
            image = self.get_url_content(figure_url)
        except Exception:
            image = None
        title = section_content.h1.text.strip()

        content = section_content.select("div.article_full_text")
        content = "\n".join(p.text.strip() for p in content)
        if content == "":
            content = section_content.select("div.article_truncated_text")[
                0
            ].text.strip()

        try:
            tag = section_content.h3.select("a")[-1].text.strip()
        except:
            tag = None
        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.archive: self.archive,
        }
        return data


@Registry.register()
class RFI(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.rfi.fr/fr/archives/{date}"
        self._base_url = "https://www.rfi.fr"

        self.archive = Archives.rfi
        self.content_selector = "main div.o-archive-day > ul > li"
        self.min_date = datetime.strptime("06-10-2009", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="y/MM/dd-MMMM-y", locale="fr")
        self.is_dynamic = {"page": False, "section": False}
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def get_section_url(self, section):
        return self._base_url + section.a.get("href")

    def parse_single_section(self, section, section_url):
        url_content = self.get_url_content(section_url)
        section_content = BeautifulSoup(url_content, "html.parser")

        try:
            figure_url = section_content.figure.picture.img.get("src")
            image = self.get_url_content(figure_url)
        except Exception:
            image = None
        title = section_content.h1.text.strip()

        content = section_content.article.p.text.strip()
        try:
            tag = section_content.article.span.text.strip()
        except:
            tag = None
        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.archive: self.archive,
        }
        return data


@Registry.register()
class FranceInfo(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.francetvinfo.fr/archives/{date}.html"
        self._base_url = "https://www.francetvinfo.fr/"

        self.archive = Archives.franceinfo
        self.content_selector = "main section> ul > li > article"
        self.min_date = datetime.strptime("01-01-2009", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="y/dd-MMMM-y", locale="fr")
        self.is_dynamic = {"page": False, "section": False}
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def get_section_url(self, section):
        return self._base_url + section.a.get("href")

    def parse_single_section(self, section, section_url):
        url_content = self.get_url_content(section_url)
        section_content = BeautifulSoup(url_content, "html.parser")

        try:
            figure_url = section_content.figure.picture.img.get("src")
            image = self.get_url_content(figure_url)
        except Exception:
            image = None
        title = section_content.h1.text.strip()

        content = section_content.select("article div.c-chapo")[0].text.strip()

        tag = None
        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.archive: self.archive,
        }
        return data


@Registry.register()
class LAlsace(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.lalsace.fr/archives/{date}"
        self._base_url = "https://www.lalsace.fr"

        self.archive = Archives.lalsace
        self.content_selector = "article"
        self.min_date = datetime.strptime("01-01-2018", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="y/dd-MM")
        self.is_dynamic = {"page": False, "section": False}
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def get_section_url(self, section):
        return self._base_url + section.a.get("href")

    def parse_single_section(self, section, section_url):
        url_content = self.get_url_content(section_url)
        section_content = BeautifulSoup(url_content, "html.parser")

        try:
            figure_url = section_content.article.figure.a.get("href")
            image = self.get_url_content(figure_url)
        except Exception:
            figure_url = image = None

        title = section_content.h1.text.strip().split("\n")[-1].strip()

        content = section_content.select("div.chapo")[0].text.strip()

        tag = section_content.h1.span.text.strip()
        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.archive: self.archive,
        }
        return data


@Registry.register()
class France24(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.france24.com/fr/archives/{date}"
        self._base_url = "https://www.france24.com/"

        self.archive = Archives.france24
        self.content_selector = "div.o-archive-day > ul > li"
        self.min_date = datetime.strptime("01-01-2007", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="y/MM/dd-MMMM-y", locale="fr")
        self.is_dynamic = {"page": False, "section": False}
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def get_section_url(self, section):
        return self._base_url + section.a.get("href")

    def parse_single_section(self, section, section_url):
        url_content = self.get_url_content(section_url)
        section_content = BeautifulSoup(url_content, "html.parser")

        try:
            figure_url = section_content.img.get("src")
            image = self.get_url_content(figure_url)
        except Exception:
            figure_url = image = None

        title = section_content.h1.text.strip()

        content = section_content.select("main > div > div > p")[0].text.strip()
        try:
            tag = section_content.select("main div.m-master-tag")[0].text.strip()
            tag = re.sub("\s+", " ", tag)
        except:
            tag = None
        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.archive: self.archive,
        }
        return data


@Registry.register()
class MondeDiplomatique(DataCollector):
    def __init__(self, begin_date, end_date, timeout):
        url_format = "https://www.monde-diplomatique.fr/{date}"
        self._base_url = "https://www.monde-diplomatique.fr"

        self.archive = Archives.mondediplomatique
        self.content_selector = "#contenu ul > li"
        self.min_date = datetime.strptime("01-01-1954", "%d-%m-%Y").date()
        date2str = partial(format_datetime, format="y/MM/")
        self.is_dynamic = {"page": False, "section": False}
        super().__init__(url_format, date2str, begin_date, end_date, timeout)

    def get_section_url(self, section):
        return self._base_url + section.a.get("href")

    def parse_single_section(self, section, section_url):
        url_content = self.get_url_content(section_url)
        section_content = BeautifulSoup(url_content, "html.parser")

        try:
            figure_url = self._base_url + "/" + section_content.figure.img.get("src")
            image = self.get_url_content(figure_url)
        except Exception:
            figure_url = image = None

        title = section_content.h1.text.strip()
        content = section_content.select("p")[1].text.strip()
        try:
            tag = section_content.select("p")[0].text.strip()
        except:
            tag = None
        data = {
            DBCOLUMNS.image: image,
            DBCOLUMNS.title: title,
            DBCOLUMNS.content: content,
            DBCOLUMNS.tag: tag,
            DBCOLUMNS.archive: self.archive,
        }
        return data
