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
    lhumanite = "lhumanite"
    lepoint = "lepoint"
    lorient = "lorientlejour"


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
    embedding = "embedding"


headers = {
"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0",
"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
"Accept-Language": "en-US,en;q=0.5",
"Accept-Encoding": "gzip, deflate",
"Connection": "keep-alive",
"Upgrade-Insecure-Requests": "1",
"Sec-Fetch-Dest": "document",
"Sec-Fetch-Mode": "navigate",
"Sec-Fetch-Site": "none",
"Sec-Fetch-User": "?1",
"Cache-Control": "max-age=0"
        }
