"""
Articles API - Search endpoint with decoupled Search and Presentation phases.

Search Strategies:
- SEMANTIC (query + min_similarity): Cache IDs in Redis, offset-based pagination
- KEYWORD (query only, min_similarity=0): Direct tsvector search with keyset pagination
- BROWSE (no query): Direct DB filtering with keyset pagination
"""

import logging
from typing import Optional, List
from datetime import date
from fastapi import APIRouter, Depends, Query, HTTPException

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_async_session
from app.core.enums import DBCOLUMNS, OPERATORS, Archives, SearchMode
from app.core.s3_client import sync_s3_client
from app.core.utils import translator, query_embedder
from app.core.redis_client import search_session_cache
from app.services.article_service import ArticleService
from app.schemas import (
    ArticleListResponse,
    ArticleResponse,
    GroupByResponse,
    ArchiveCountResponse,
    TagsResponse,
    DateRangeResponse,
    FilterParams,
)

router = APIRouter()
logger = logging.getLogger(__name__)


def determine_search_mode(
    query: Optional[str],
    min_similarity: float,
) -> SearchMode:
    """
    Determine which search strategy to use based on query parameters.

    Returns:
        SearchMode.SEMANTIC: query + min_similarity > 0 (vector search with Redis cache)
        SearchMode.KEYWORD: query + min_similarity = 0 (fast tsvector)
        SearchMode.BROWSE: no query (fast filters only)
    """
    if query and min_similarity > 0:
        return SearchMode.SEMANTIC
    elif query:
        return SearchMode.KEYWORD
    else:
        return SearchMode.BROWSE


def build_non_semantic_filters(
    archives: Optional[List[str]] = None,
    tag: Optional[str] = None,
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
    has_image: Optional[bool] = None,
    translated_query: Optional[str] = None,
) -> dict:
    """
    Build filter dictionary for non-semantic operations (keyword search, browse, counts).
    Does not include embedding filter.
    """
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

    if has_image:
        filters[DBCOLUMNS.image] = [(OPERATORS.notnull, None)]

    if translated_query:
        filters[DBCOLUMNS.text_searchable] = [(OPERATORS.ts, translated_query)]

    return filters


async def build_filters(
    archives: Optional[List[str]] = None,
    tag: Optional[str] = None,
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
    query: Optional[str] = None,
    has_image: Optional[bool] = None,
    min_similarity: Optional[float] = None,
) -> dict:
    """
    Build filter dictionary from parameters (legacy method for non-search endpoints).

    If min_similarity is provided with a query, uses semantic search.
    Otherwise, uses BM25 text search for the query.
    """
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

    if has_image:
        filters[DBCOLUMNS.image] = [(OPERATORS.notnull, None)]

    # Search handling: semantic if min_similarity provided, otherwise BM25
    if query:
        if min_similarity is not None:
            # Semantic search: get embedding and filter by similarity threshold
            try:
                query_embedding = await query_embedder.embed_async(query)
                filters[DBCOLUMNS.embedding] = [(OPERATORS.semantic, (query_embedding, min_similarity))]
            except Exception as e:
                logger.error(f"Failed to get query embedding: {e}")
                raise HTTPException(
                    status_code=503,
                    detail="Embedding service unavailable. Please try again later."
                )
        else:
            # BM25 text search
            translated_query = translator.to_french(query)
            filters[DBCOLUMNS.text_searchable] = [(OPERATORS.ts, translated_query)]

    return filters


def add_image_urls(articles: List[dict], similarities: Optional[dict] = None) -> List[ArticleResponse]:
    """Generate presigned S3 URLs for article images and add similarity scores."""
    articles_with_urls = []
    for article in articles:
        image_url = None
        if article.get("image"):
            image_url = sync_s3_client.get_presigned_url(
                article["image"], expires_in=3600
            )
        # Add similarity score if available
        similarity = None
        if similarities:
            rowid = article.get("rowid")
            similarity = similarities.get(str(rowid)) or similarities.get(rowid)
        articles_with_urls.append(ArticleResponse(**article, image_url=image_url, similarity=similarity))
    return articles_with_urls


@router.get("", response_model=ArticleListResponse)
async def get_articles(
    # Filter parameters
    archives: Optional[List[str]] = Query(None),
    tag: Optional[str] = None,
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
    query: Optional[str] = None,
    has_image: Optional[bool] = None,
    min_similarity: float = Query(
        default=0.5,
        ge=0,
        le=1,
        description="Minimum similarity threshold for semantic search (0-1)."
    ),
    # Pagination parameters (keyset for keyword/browse, offset for semantic)
    limit: int = Query(default=30, ge=1, le=100),
    direction: str = Query(default="forward", pattern="^(forward|backward)$"),
    desc_order: bool = True,
    last_seen_date: Optional[date] = None,
    last_seen_rowid: Optional[int] = None,
    # Semantic search session (for paginating cached results)
    search_session_id: Optional[str] = Query(
        default=None,
        description="Session ID for paginating semantic search results. "
                    "Returned on first semantic search request."
    ),
    page: int = Query(
        default=1,
        ge=1,
        description="Page number for semantic search pagination (1-indexed). "
                    "Only used when search_session_id is provided."
    ),
    session: AsyncSession = Depends(get_async_session),
):
    """
    Fetch articles with filters and pagination.

    Search modes:
    - **Semantic Search** (query + min_similarity): First request performs vector search
      and caches IDs in Redis. Returns search_session_id for pagination. Subsequent
      requests with search_session_id use offset-based pagination from cache.
    - **Keyword Search** (query only): BM25 full-text search with keyset pagination.
    - **Browse Mode** (no query): Filter and browse with keyset pagination.
    """
    try:
        search_mode = determine_search_mode(query, min_similarity)
        logger.info(
            f"Search request: mode={search_mode.value}, query='{query}', "
            f"min_similarity={min_similarity}, session_id={search_session_id}"
        )
        service = ArticleService(session)

        # =====================================================================
        # CASE A: SEMANTIC SEARCH (Vector Embeddings)
        # Strategy: Snapshot Pattern - Cache IDs in Redis, offset-based pagination
        # =====================================================================
        if search_mode == SearchMode.SEMANTIC:
            # Check if we have a cached session to paginate
            if search_session_id:
                # PRESENTATION PHASE: Retrieve cached IDs and paginate
                cached = await search_session_cache.get_search_results(search_session_id)

                if cached is None:
                    raise HTTPException(
                        status_code=404,
                        detail="Search session expired or not found. Please perform a new search."
                    )

                article_ids = cached["article_ids"]
                total_count = cached["total_count"]
                similarities = cached.get("similarities", {})

                # Refresh TTL on access
                await search_session_cache.refresh_session_ttl(search_session_id)

                # Calculate offset from page number (1-indexed)
                offset = (page - 1) * limit

                # Fetch articles by IDs (preserves similarity order)
                articles = await service.fetch_articles_by_ids(
                    article_ids=article_ids,
                    offset=offset,
                    limit=limit,
                    preserve_order=True,
                )

                articles_with_urls = add_image_urls(articles, similarities)

                # Calculate if there are more pages
                has_more = (page * limit) < total_count

                return ArticleListResponse(
                    articles=articles_with_urls,
                    total_count=total_count,
                    last_seen=None,  # Not used for semantic search
                    has_more=has_more,
                    search_session_id=search_session_id,
                    page=page,
                )

            else:
                # SEARCH PHASE: Compute embedding, perform vector search, cache results
                try:
                    query_embedding = await query_embedder.embed_async(query)
                except Exception as e:
                    logger.error(f"Failed to get query embedding: {e}")
                    raise HTTPException(
                        status_code=503,
                        detail="Embedding service unavailable. Please try again later."
                    )

                # Build non-semantic filters for additional filtering
                non_semantic_filters = build_non_semantic_filters(
                    archives=archives,
                    tag=tag,
                    date_start=date_start,
                    date_end=date_end,
                    has_image=has_image,
                )

                # Perform vector search to get all matching IDs (ordered by similarity)
                article_ids, total_count, similarities = await service.semantic_search_ids(
                    query_embedding=query_embedding,
                    min_similarity=min_similarity,
                    filters=non_semantic_filters if non_semantic_filters else None,
                )

                if not article_ids:
                    return ArticleListResponse(
                        articles=[],
                        total_count=0,
                        last_seen=None,
                        has_more=False,
                        search_session_id=None,
                        page=1,
                    )

                # Generate session ID and cache results (including similarities)
                new_session_id = search_session_cache.generate_session_id()
                await search_session_cache.store_search_results(
                    session_id=new_session_id,
                    article_ids=article_ids,
                    total_count=total_count,
                    similarities=similarities,
                )

                # Return first page (ordered by similarity)
                articles = await service.fetch_articles_by_ids(
                    article_ids=article_ids,
                    offset=0,
                    limit=limit,
                    preserve_order=True,
                )

                articles_with_urls = add_image_urls(articles, similarities)

                # Calculate if there are more pages
                has_more = limit < total_count

                return ArticleListResponse(
                    articles=articles_with_urls,
                    total_count=total_count,
                    last_seen=None,
                    has_more=has_more,
                    search_session_id=new_session_id,
                    page=1,
                )

        # =====================================================================
        # CASE B: KEYWORD SEARCH (tsvector) OR BROWSE MODE (No Query)
        # Strategy: Direct DB Access with keyset pagination
        # =====================================================================
        else:
            # Build filters (including text search for keyword mode)
            translated_query = None
            if search_mode == SearchMode.KEYWORD and query:
                translated_query = translator.to_french(query)

            filters = build_non_semantic_filters(
                archives=archives,
                tag=tag,
                date_start=date_start,
                date_end=date_end,
                has_image=has_image,
                translated_query=translated_query,
            )

            # Direct keyset pagination
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

            articles_with_urls = add_image_urls(articles)

            # For keyset pagination, has_more if we got a full page
            has_more = len(articles) == limit

            return ArticleListResponse(
                articles=articles_with_urls,
                total_count=total_count,
                last_seen=last_seen,
                has_more=has_more,
                search_session_id=None,  # Not used for keyword/browse
                page=None,
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching articles: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/count")
async def get_article_count(
    archives: Optional[List[str]] = Query(None),
    tag: Optional[str] = None,
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
    query: Optional[str] = None,
    has_image: Optional[bool] = None,
    min_similarity: Optional[float] = Query(default=None, ge=0, le=1),
    session: AsyncSession = Depends(get_async_session),
):
    """Get total article count with filters."""
    try:
        filters = await build_filters(
            archives=archives,
            tag=tag,
            date_start=date_start,
            date_end=date_end,
            query=query,
            has_image=has_image,
            min_similarity=min_similarity,
        )

        service = ArticleService(session)
        count = await service.get_total_count(filters if filters else None)

        return {"count": count}
    except HTTPException:
        raise
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
    min_similarity: Optional[float] = Query(default=None, ge=0, le=1),
    session: AsyncSession = Depends(get_async_session),
):
    """Get articles grouped by day, month, or year."""
    try:
        filters = await build_filters(
            archives=archives,
            tag=tag,
            date_start=date_start,
            date_end=date_end,
            query=query,
            has_image=has_image,
            min_similarity=min_similarity,
        )

        service = ArticleService(session)
        data = await service.group_by(group_by, filters if filters else None)

        return GroupByResponse(data=data)
    except HTTPException:
        raise
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


@router.get("/archive-counts", response_model=ArchiveCountResponse)
async def get_archive_counts(
    archives: Optional[List[str]] = Query(None),
    tag: Optional[str] = None,
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
    query: Optional[str] = None,
    has_image: Optional[bool] = None,
    min_similarity: Optional[float] = Query(default=None, ge=0, le=1),
    session: AsyncSession = Depends(get_async_session),
):
    """Get article counts per archive with filters."""
    try:
        filters = await build_filters(
            archives=archives,
            tag=tag,
            date_start=date_start,
            date_end=date_end,
            query=query,
            has_image=has_image,
            min_similarity=min_similarity,
        )

        service = ArticleService(session)
        data = await service.get_archive_counts(filters if filters else None)

        return ArchiveCountResponse(data=data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting archive counts: {e}")
        raise HTTPException(status_code=500, detail=str(e))
