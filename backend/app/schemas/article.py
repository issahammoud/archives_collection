from datetime import date
from typing import Optional, List, Any
from pydantic import BaseModel, Field


class ArticleBase(BaseModel):
    date: date
    archive: str
    image: Optional[str] = None
    title: Optional[str] = None
    content: Optional[str] = None
    tag: Optional[str] = None
    link: str


class ArticleCreate(ArticleBase):
    embedding: Optional[List[float]] = None


class ArticleResponse(ArticleBase):
    rowid: int

    class Config:
        from_attributes = True


class ArticleListResponse(BaseModel):
    articles: List[ArticleResponse]
    total_count: int
    last_seen: Optional[dict] = None


class FilterParams(BaseModel):
    archives: Optional[List[str]] = None
    tag: Optional[str] = None
    date_start: Optional[date] = None
    date_end: Optional[date] = None
    query: Optional[str] = None
    has_image: Optional[bool] = None


class PaginationParams(BaseModel):
    limit: int = Field(default=30, ge=1, le=100)
    direction: str = Field(default="forward", pattern="^(forward|backward)$")
    desc_order: bool = True
    last_seen_date: Optional[date] = None
    last_seen_rowid: Optional[int] = None


class GroupByResponse(BaseModel):
    data: List[dict]


class TagsResponse(BaseModel):
    tags: List[str]


class DateRangeResponse(BaseModel):
    min_date: Optional[date] = None
    max_date: Optional[date] = None


class TaskResponse(BaseModel):
    task_id: str
    status: str
    task_name: str


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    result: Optional[Any] = None


class CollectionRequest(BaseModel):
    archives: Optional[List[str]] = None
    begin_date: date
    end_date: date


class DownloadRequest(BaseModel):
    filters: Optional[FilterParams] = None
    order: bool = True
