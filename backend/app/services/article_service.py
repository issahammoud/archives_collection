"""
Article Service - Handles data access with strategy-based search.

Search Strategies:
- SEMANTIC: Vector search -> Cache IDs in Redis -> Paginate from cache
- KEYWORD/BROWSE: Direct SQL filtering with keyset pagination
"""

import logging
from typing import Optional, List, Tuple
from datetime import date

from sqlalchemy import select, func, or_, and_, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.article import Article, ArticleEmbedding
from app.core.enums import DBCOLUMNS, OPERATORS
from app.core.config import settings

logger = logging.getLogger(__name__)


class ArticleService:
    def __init__(self, session: AsyncSession):
        self.session = session

    def _apply_filters(
        self,
        query,
        filters: Optional[dict] = None,
        skip_semantic: bool = False,
    ):
        """
        Apply filters to a SQLAlchemy query.

        Args:
            query: The SQLAlchemy query to modify
            filters: Dict of column -> [(operator, value)] pairs
            skip_semantic: If True, skip the semantic filter (used for ID-based lookups)

        Returns the modified query.
        """
        if not filters:
            return query

        for column, conditions in filters.items():
            for operator, value in conditions:
                if column == DBCOLUMNS.date:
                    if operator == OPERATORS.ge:
                        query = query.where(Article.date >= value)
                    elif operator == OPERATORS.le:
                        query = query.where(Article.date <= value)
                elif column == DBCOLUMNS.tag:
                    if operator == OPERATORS.like:
                        query = query.where(
                            func.upper(Article.tag).like(f"%{value.upper()}%")
                        )
                elif column == DBCOLUMNS.archive:
                    if operator == OPERATORS.in_:
                        query = query.where(Article.archive.in_(value))
                elif column == DBCOLUMNS.image:
                    if operator == OPERATORS.notnull:
                        query = query.where(Article.image.isnot(None))
                        query = query.where(Article.image != "")
                elif column == DBCOLUMNS.text_searchable:
                    if operator == OPERATORS.ts:
                        ts_query = func.websearch_to_tsquery("french", func.unaccent(value))
                        query = query.where(Article.text_searchable.op("@@")(ts_query))
                elif column == DBCOLUMNS.embedding and not skip_semantic:
                    if operator == OPERATORS.semantic:
                        query_embedding, min_similarity = value
                        query = query.where(ArticleEmbedding.embedding.isnot(None))
                        query = query.where(
                            ArticleEmbedding.embedding.max_inner_product(query_embedding) <= -min_similarity
                        )

        return query

    def _needs_embedding_join(self, filters: Optional[dict] = None) -> bool:
        """Check if filters require joining with article_embeddings table."""
        if not filters:
            return False
        return DBCOLUMNS.embedding in filters

    # =========================================================================
    # SEMANTIC SEARCH: Cache-Based Strategy (Snapshot Pattern)
    # =========================================================================

    async def semantic_search_ids(
        self,
        query_embedding: List[float],
        min_similarity: float,
        filters: Optional[dict] = None,
        max_results: int = 1000,
    ) -> Tuple[List[int], int, dict]:
        """
        Perform semantic vector search and return article IDs ordered by relevance.

        This is the expensive operation that should only run once per search session.
        Results are cached in Redis for subsequent pagination requests.

        Args:
            query_embedding: The query vector from embedding service
            min_similarity: Minimum similarity threshold (0-1)
            filters: Additional filters (date, archive, tag, etc.)
            max_results: Maximum number of IDs to return (caps memory usage)

        Returns:
            Tuple of (list of article rowids ordered by similarity, total count, similarity scores dict)
        """
        try:
            # Set HNSW ef_search for better recall (higher = more accurate, slower)
            # Using SET LOCAL so it works with PgBouncer transaction pooling
            await self.session.execute(
                text(f"SET LOCAL hnsw.ef_search = {settings.HNSW_EF_SEARCH}")
            )

            # Verify ef_search is set (can remove after confirming)
            result = await self.session.execute(text("SHOW hnsw.ef_search"))
            current_ef = result.scalar()
            logger.info(f"HNSW ef_search set to: {current_ef}")

            # Build query selecting rowid, filtered by similarity threshold
            similarity_expr = ArticleEmbedding.embedding.max_inner_product(query_embedding)

            query = (
                select(Article.rowid, Article.date, (-similarity_expr).label("similarity"))
                .join(ArticleEmbedding, Article.rowid == ArticleEmbedding.rowid)
                .where(ArticleEmbedding.embedding.isnot(None))
                .where(similarity_expr <= -min_similarity)
            )

            # Apply non-semantic filters (skip_semantic=True since we handle it above)
            query = self._apply_filters(query, filters, skip_semantic=True)

            # Order by (date DESC, rowid DESC) - same as keyset pagination
            query = query.order_by(Article.date.desc(), Article.rowid.desc()).limit(max_results)

            result = await self.session.execute(query)
            rows = result.fetchall()

            article_ids = [row.rowid for row in rows]
            similarities = {row.rowid: row.similarity for row in rows}
            total_count = len(article_ids)

            logger.info(
                f"Semantic search returned {total_count} IDs "
                f"(min_similarity={min_similarity}, max_results={max_results})"
            )

            return article_ids, total_count, similarities

        except Exception as e:
            logger.error(f"Error in semantic_search_ids: {e}", exc_info=True)
            return [], 0, {}

    async def fetch_articles_by_ids(
        self,
        article_ids: List[int],
        offset: int = 0,
        limit: int = 30,
        preserve_order: bool = True,
    ) -> List[dict]:
        """
        Fetch articles by their IDs with offset-based pagination.

        Used for the Presentation Phase of semantic search where IDs are cached.
        The order of results matches the order of input IDs (relevance order).

        Args:
            article_ids: List of article rowids (already ordered by relevance)
            offset: Number of items to skip
            limit: Number of items to return
            preserve_order: If True, maintain the order of article_ids in results

        Returns:
            List of article dicts in the same order as input IDs
        """
        try:
            if not article_ids:
                return []

            # Apply pagination to the ID list
            paginated_ids = article_ids[offset : offset + limit]

            if not paginated_ids:
                return []

            # Fetch articles by IDs
            query = select(Article).where(Article.rowid.in_(paginated_ids))
            result = await self.session.execute(query)
            rows = result.scalars().all()

            # Create a lookup dict for ordering
            if preserve_order:
                row_dict = {row.rowid: row for row in rows}
                ordered_rows = [row_dict[rid] for rid in paginated_ids if rid in row_dict]
            else:
                ordered_rows = rows

            logger.info(
                f"fetch_articles_by_ids: offset={offset}, limit={limit}, "
                f"returned {len(ordered_rows)} articles"
            )

            return self._serialize_articles(ordered_rows)

        except Exception as e:
            logger.error(f"Error in fetch_articles_by_ids: {e}", exc_info=True)
            return []

    # =========================================================================
    # KEYWORD/BROWSE SEARCH: Direct DB Strategy (Keyset Pagination)
    # =========================================================================

    async def fetch_data_keyset(
        self,
        limit: int = 30,
        direction: str = "forward",
        desc_order: bool = True,
        last_seen_date: Optional[date] = None,
        last_seen_rowid: Optional[int] = None,
        filters: Optional[dict] = None,
        columns: Optional[List[str]] = None,
    ) -> List[dict]:
        """
        Fetch articles with keyset pagination (for keyword search and browse mode).

        This method queries the database directly without caching because:
        - Keyword search (tsvector) is fast with GIN indexes
        - Browse mode is simple filtering

        Args:
            limit: Number of articles to return
            direction: "forward" or "backward" pagination
            desc_order: True for newest first, False for oldest first
            last_seen_date: Cursor position date
            last_seen_rowid: Cursor position rowid
            filters: Dict of filters to apply

        Returns:
            List of article dicts
        """
        try:
            logger.info(
                f"fetch_data_keyset: direction={direction}, desc_order={desc_order}, "
                f"last_seen_date={last_seen_date}, last_seen_rowid={last_seen_rowid}"
            )

            query = select(Article)

            if self._needs_embedding_join(filters):
                query = query.join(ArticleEmbedding, Article.rowid == ArticleEmbedding.rowid)

            query = self._apply_filters(query, filters)

            is_effective_desc = (direction == "forward" and desc_order) or (
                direction == "backward" and not desc_order
            )

            if last_seen_date is not None and last_seen_rowid is not None:
                if is_effective_desc:
                    query = query.where(
                        or_(
                            Article.date < last_seen_date,
                            and_(
                                Article.date == last_seen_date,
                                Article.rowid < last_seen_rowid,
                            ),
                        )
                    )
                else:
                    query = query.where(
                        or_(
                            Article.date > last_seen_date,
                            and_(
                                Article.date == last_seen_date,
                                Article.rowid > last_seen_rowid,
                            ),
                        )
                    )

            if is_effective_desc:
                query = query.order_by(Article.date.desc(), Article.rowid.desc())
            else:
                query = query.order_by(Article.date.asc(), Article.rowid.asc())

            query = query.limit(limit)

            result = await self.session.execute(query)
            rows = result.scalars().all()

            if direction == "backward":
                rows = list(reversed(rows))

            logger.info(f"fetch_data_keyset returned {len(rows)} rows")

            return self._serialize_articles(rows)

        except Exception as e:
            logger.error(f"Error in fetch_data_keyset: {e}", exc_info=True)
            return []

    # =========================================================================
    # COUNT METHODS
    # =========================================================================

    async def get_total_count(self, filters: Optional[dict] = None) -> int:
        """Get total count of articles matching filters."""
        try:
            query = select(func.count()).select_from(Article)

            if self._needs_embedding_join(filters):
                query = query.join(ArticleEmbedding, Article.rowid == ArticleEmbedding.rowid)

            query = self._apply_filters(query, filters)
            result = await self.session.execute(query)
            count = result.scalar()
            logger.info(f"get_total_count returned: {count}")
            return count or 0
        except Exception as e:
            logger.error(f"Error in get_total_count: {e}", exc_info=True)
            return 0

    async def get_count_for_ids(self, article_ids: List[int]) -> int:
        """Get count for cached semantic search (just return length of ID list)."""
        return len(article_ids)

    # =========================================================================
    # AGGREGATION METHODS
    # =========================================================================

    async def group_by(self, value: str, filters: Optional[dict] = None) -> List[dict]:
        """Group articles by day, month, or year."""
        if value not in ["day", "month", "year"]:
            raise ValueError("value must be 'day', 'month', or 'year'")

        try:
            query = select(
                func.date_trunc(value, Article.date).label("period"),
                func.count().label("count")
            ).select_from(Article)

            if self._needs_embedding_join(filters):
                query = query.join(ArticleEmbedding, Article.rowid == ArticleEmbedding.rowid)

            query = self._apply_filters(query, filters)
            query = query.group_by("period").order_by("period")

            result = await self.session.execute(query)
            rows = result.fetchall()

            return [
                {"date": row.period.isoformat() if row.period else None, "count": row.count}
                for row in rows
            ]
        except Exception as e:
            logger.error(f"Error in group_by: {e}")
            return []

    async def get_tags(self, filters: Optional[dict] = None) -> List[str]:
        """Get top 100 tags by frequency."""
        try:
            query = (
                select(func.trim(func.upper(Article.tag)))
                .where(Article.tag.isnot(None))
                .group_by(func.trim(func.upper(Article.tag)))
                .order_by(func.count().desc())
                .limit(100)
            )

            result = await self.session.execute(query)
            rows = result.fetchall()

            return [
                row[0].title() if row[0] and not row[0].isnumeric() else None
                for row in rows
                if row[0] and not row[0].isnumeric()
            ]
        except Exception as e:
            logger.error(f"Error in get_tags: {e}")
            return []

    async def get_min_max_dates(self, filters: Optional[dict] = None) -> tuple:
        """Get min and max dates in the database."""
        try:
            query = select(func.min(Article.date), func.max(Article.date))

            result = await self.session.execute(query)
            row = result.fetchone()

            if row:
                logger.info(f"get_min_max_dates returned: min={row[0]}, max={row[1]}")
                return row[0], row[1]
            logger.info("get_min_max_dates returned no rows")
            return None, None
        except Exception as e:
            logger.error(f"Error in get_min_max_dates: {e}", exc_info=True)
            return None, None

    async def get_archive_counts(self, filters: Optional[dict] = None) -> List[dict]:
        """Get article counts per archive with optional filters."""
        try:
            query = select(
                Article.archive,
                func.count().label("count")
            ).select_from(Article)

            if self._needs_embedding_join(filters):
                query = query.join(ArticleEmbedding, Article.rowid == ArticleEmbedding.rowid)

            query = self._apply_filters(query, filters)
            query = query.group_by(Article.archive).order_by(func.count().desc())

            result = await self.session.execute(query)
            rows = result.fetchall()

            return [{"archive": row.archive, "count": row.count} for row in rows if row.archive]
        except Exception as e:
            logger.error(f"Error in get_archive_counts: {e}")
            return []

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _serialize_articles(self, rows) -> List[dict]:
        """Convert Article ORM objects to dicts."""
        articles = []
        for row in rows:
            articles.append(
                {
                    "rowid": row.rowid,
                    "date": row.date.isoformat() if row.date else None,
                    "archive": row.archive,
                    "image": (
                        row.image.replace("/images/", "")
                        if isinstance(row.image, str)
                        else row.image
                    ),
                    "title": row.title,
                    "content": row.content,
                    "tag": row.tag,
                    "link": row.link,
                }
            )
        return articles
