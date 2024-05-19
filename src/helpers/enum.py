from enum import Enum


class Archives(str, Enum):
    lemonde = "lemonde"
    lefigaro = "lefigaro"
    lesechos = "lesechos"
    vinghtminutes = "vinghtminutes"
    ouestfrance = "ouestfrance"
    liberation = "liberation"
    mediapart = "mediapart"
    leparisien = "leparisien"


class DBCOLUMNS(str, Enum):
    rowid = "rowid"
    date = "date"
    archive = "archive"
    image = "image"
    title = "title"
    content = "content"
    tag = "tag"
    link = "link"
    text_searchable = "text_searchable"


headers = {
    "accept": "text/html,application/xhtml+xml,"
    "application/xml;q=0.9,image/avif,"
    "image/webp,image/apng,*/*;q=0.8,"
    "application/signed-exchange;v=b3;q=0.7",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "max-age=0",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/115.0.0.0 Safari/537.36",
}
