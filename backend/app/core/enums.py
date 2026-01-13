from enum import Enum


class Archives(str, Enum):
    lemonde = "lemonde"
    lesechos = "lesechos"
    vinghtminutes = "vinghtminutes"
    leparisien = "leparisien"
    lepoint = "lepoint"
    lorient = "lorient"
    rfi = "rfi"
    franceinfo = "franceinfo"
    lalsace = "lalsace"
    france24 = "france24"


class DBCOLUMNS(str, Enum):
    rowid = "rowid"
    date = "date"
    archive = "archive"
    image = "image"
    title = "title"
    content = "content"
    tag = "tag"
    link = "link"
    hash = "hash"
    embedding = "embedding"
    text_searchable = "text_searchable"


class OPERATORS(str, Enum):
    eq = "eq"
    gt = "gt"
    lt = "lt"
    ge = "ge"
    le = "le"
    in_ = "in"
    like = "like"
    isnull = "isnull"
    notnull = "notnull"
    ts = "text_search"
    vs = "vector_search"


class CeleryTasks(str, Enum):
    collect = "collect"
    download = "download"


class JobStatus(str, Enum):
    PENDING = "PENDING"
    STARTED = "STARTED"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    REVOKED = "REVOKED"
