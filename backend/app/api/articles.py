import logging
from typing import Optional, List
from datetime import date
from fastapi import APIRouter, Depends, Query, HTTPException

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_async_session
from app.core.enums import DBCOLUMNS, OPERATORS, Archives
from app.core.utils import get_query_embedding_async
from app.services.article_service import ArticleService
from app.schemas import (
    ArticleListResponse,
    ArticleResponse,
    GroupByResponse,
    TagsResponse,
    DateRangeResponse,
    FilterParams,
)

router = APIRouter()
logger = logging.getLogger(__name__)


def build_filters(
    archives: Optional[List[str]] = None,
    tag: Optional[str] = None,
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
    query: Optional[str] = None,
    has_image: Optional[bool] = None,
    embedding: Optional[List[float]] = None,
) -> dict:
    """Build filter dictionary from parameters."""
    filters = {}

    if date_start or date_end:
        filters[DBCOLUMNS.date] = []
        if date_start:
            filters[DBCOLUMNS.date].append((OPERATORS.ge, date_start))
        if date_end:
            filters[DBCOLUMNS.date].append((OPERATORS.le, date_end))

    if tag:
        filters[DBCOLUMNS.tag] = [(OPERATORS.like, tag.strip())]

    if archives:
        filters[DBCOLUMNS.archive] = [(OPERATORS.in_, archives)]

    if query:
        filters[DBCOLUMNS.text_searchable] = [(OPERATORS.ts, query)]
        if embedding:
            filters[DBCOLUMNS.embedding] = [(OPERATORS.vs, embedding)]

    if has_image:
        filters[DBCOLUMNS.image] = [(OPERATORS.notnull, None)]

    return filters


@router.get("", response_model=ArticleListResponse)
async def get_articles(
    archives: Optional[List[str]] = Query(None),
    tag: Optional[str] = None,
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
    query: Optional[str] = None,
    has_image: Optional[bool] = None,
    limit: int = Query(default=30, ge=1, le=100),
    direction: str = Query(default="forward", pattern="^(forward|backward)$"),
    desc_order: bool = True,
    last_seen_date: Optional[date] = None,
    last_seen_rowid: Optional[int] = None,
    session: AsyncSession = Depends(get_async_session),
):
    """Fetch articles with filters and pagination."""
    try:
        embedding = None
        if query:
            embedding = await get_query_embedding_async(query)

        filters = build_filters(
            archives=archives,
            tag=tag,
            date_start=date_start,
            date_end=date_end,
            query=query,
            has_image=has_image,
            embedding=embedding,
        )

        service = ArticleService(session)

        articles = await service.fetch_data_keyset(
            limit=limit,
            direction=direction,
            desc_order=desc_order,
            last_seen_date=last_seen_date,
            last_seen_rowid=last_seen_rowid,
            filters=filters if filters else None,
        )

        total_count = await service.get_total_count(filters if filters else None)

        last_seen = None
        if articles:
            last_article = articles[-1]
            first_article = articles[0]
            last_seen = {
                "forward": {
                    "date": last_article.get("date"),
                    "rowid": last_article.get("rowid"),
                },
                "backward": {
                    "date": first_article.get("date"),
                    "rowid": first_article.get("rowid"),
                },
            }

        return ArticleListResponse(
            articles=[ArticleResponse(**a) for a in articles],
            total_count=total_count,
            last_seen=last_seen,
        )
    except Exception as e:
        logger.error(f"Error fetching articles: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/count")
async def get_article_count(
    archives: Optional[List[str]] = Query(None),
    tag: Optional[str] = None,
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
    query: Optional[str] = None,
    has_image: Optional[bool] = None,
    session: AsyncSession = Depends(get_async_session),
):
    """Get total article count with filters."""
    try:
        embedding = None
        if query:
            embedding = await get_query_embedding_async(query)

        filters = build_filters(
            archives=archives,
            tag=tag,
            date_start=date_start,
            date_end=date_end,
            query=query,
            has_image=has_image,
            embedding=embedding,
        )

        service = ArticleService(session)
        count = await service.get_total_count(filters if filters else None)

        return {"count": count}
    except Exception as e:
        logger.error(f"Error getting count: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/group-by", response_model=GroupByResponse)
async def get_grouped_articles(
    group_by: str = Query(default="day", pattern="^(day|month|year)$"),
    archives: Optional[List[str]] = Query(None),
    tag: Optional[str] = None,
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
    query: Optional[str] = None,
    has_image: Optional[bool] = None,
    session: AsyncSession = Depends(get_async_session),
):
    """Get articles grouped by day, month, or year."""
    try:
        embedding = None
        if query:
            embedding = await get_query_embedding_async(query)

        filters = build_filters(
            archives=archives,
            tag=tag,
            date_start=date_start,
            date_end=date_end,
            query=query,
            has_image=has_image,
            embedding=embedding,
        )

        service = ArticleService(session)
        data = await service.group_by(group_by, filters if filters else None)

        return GroupByResponse(data=data)
    except Exception as e:
        logger.error(f"Error grouping articles: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tags", response_model=TagsResponse)
async def get_tags(
    session: AsyncSession = Depends(get_async_session),
):
    """Get all available tags."""
    try:
        service = ArticleService(session)
        tags = await service.get_tags()

        return TagsResponse(tags=[t for t in tags if t])
    except Exception as e:
        logger.error(f"Error getting tags: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/date-range", response_model=DateRangeResponse)
async def get_date_range(
    session: AsyncSession = Depends(get_async_session),
):
    """Get min and max dates in the database."""
    try:
        service = ArticleService(session)
        min_date, max_date = await service.get_min_max_dates()

        return DateRangeResponse(min_date=min_date, max_date=max_date)
    except Exception as e:
        logger.error(f"Error getting date range: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/archives")
async def get_archives():
    """Get list of available archives."""
    return {"archives": [a.value for a in Archives]}
