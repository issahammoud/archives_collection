from enum import Enum


class Archives(str, Enum):
    lemonde = "lemonde"
    lefigaro = "lefigaro"
    lesechos = "lesechos"
    vinghtminutes = "vinghtminutes"


class DBCOLUMNS(str, Enum):
    rowid = "rowid"
    date = "date"
    archive = "archive"
    image = "image"
    title = "title"
    content = "content"
    tag = "tag"
    link = "link"
