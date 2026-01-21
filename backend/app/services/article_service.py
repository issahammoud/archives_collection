import logging
from typing import Optional, List
from datetime import date

from sqlalchemy import select, func, text, tuple_, or_, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.article import Article
from app.core.enums import DBCOLUMNS, OPERATORS

logger = logging.getLogger(__name__)


class ArticleService:
    def __init__(self, session: AsyncSession):
        self.session = session

    def _apply_filters(self, query, filters: Optional[dict] = None):
        """Apply filters to a SQLAlchemy query."""
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
                        # Full-text search using to_tsquery
                        ts_query = func.websearch_to_tsquery("french", func.unaccent(value))
                        query = query.where(Article.text_searchable.op("@@")(ts_query))

        return query

    def _build_filter_sql(self, filters: Optional[dict] = None) -> tuple:
        """Build SQL WHERE clause and params for raw queries."""
        if not filters:
            return "", {}

        clauses = []
        params = {}
        param_idx = 0

        for column, conditions in filters.items():
            for operator, value in conditions:
                if column == DBCOLUMNS.date:
                    if operator == OPERATORS.ge:
                        clauses.append(f"date >= :date_start_{param_idx}")
                        params[f"date_start_{param_idx}"] = value
                    elif operator == OPERATORS.le:
                        clauses.append(f"date <= :date_end_{param_idx}")
                        params[f"date_end_{param_idx}"] = value
                elif column == DBCOLUMNS.tag:
                    if operator == OPERATORS.like:
                        clauses.append(f"UPPER(tag) LIKE :tag_{param_idx}")
                        params[f"tag_{param_idx}"] = f"%{value.upper()}%"
                elif column == DBCOLUMNS.archive:
                    if operator == OPERATORS.in_:
                        placeholders = ", ".join(
                            f":archive_{param_idx}_{i}" for i in range(len(value))
                        )
                        clauses.append(f"archive IN ({placeholders})")
                        for i, v in enumerate(value):
                            params[f"archive_{param_idx}_{i}"] = v
                elif column == DBCOLUMNS.image:
                    if operator == OPERATORS.notnull:
                        clauses.append("image IS NOT NULL AND image != ''")
                elif column == DBCOLUMNS.text_searchable:
                    if operator == OPERATORS.ts:
                        clauses.append(
                            f"text_searchable @@ websearch_to_tsquery('french', unaccent(:query_{param_idx}))"
                        )
                        params[f"query_{param_idx}"] = value
                param_idx += 1

        where_clause = " AND ".join(clauses) if clauses else ""
        return where_clause, params

    async def get_total_count(self, filters: Optional[dict] = None) -> int:
        try:
            query = select(func.count()).select_from(Article)
            query = self._apply_filters(query, filters)
            result = await self.session.execute(query)
            count = result.scalar()
            logger.info(f"get_total_count returned: {count}")
            return count or 0
        except Exception as e:
            logger.error(f"Error in get_total_count: {e}", exc_info=True)
            return 0

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
        try:
            logger.info(
                f"fetch_data_keyset: direction={direction}, desc_order={desc_order}, "
                f"last_seen_date={last_seen_date}, last_seen_rowid={last_seen_rowid}"
            )

            query = select(Article)
            query = self._apply_filters(query, filters)

            is_effective_desc = (direction == "forward" and desc_order) or (
                direction == "backward" and not desc_order
            )

            if last_seen_date is not None and last_seen_rowid is not None:
                if is_effective_desc:
                    # (date, rowid) < (last_seen_date, last_seen_rowid)
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
                    # (date, rowid) > (last_seen_date, last_seen_rowid)
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

            # For backward navigation, reverse results to maintain display order
            if direction == "backward":
                rows = list(reversed(rows))

            logger.info(f"fetch_data_keyset returned {len(rows)} rows")

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
        except Exception as e:
            logger.error(f"Error in fetch_data_keyset: {e}", exc_info=True)
            return []

    async def group_by(self, value: str, filters: Optional[dict] = None) -> List[dict]:
        if value not in ["day", "month", "year"]:
            raise ValueError("value must be 'day', 'month', or 'year'")

        try:
            where_clause, params = self._build_filter_sql(filters)
            where_sql = f"WHERE {where_clause}" if where_clause else ""

            query = text(
                f"""
                SELECT date_trunc(:interval, date) as period, count(*) as count
                FROM articles
                {where_sql}
                GROUP BY period
                ORDER BY period
            """
            )

            params["interval"] = value
            result = await self.session.execute(query, params)
            rows = result.fetchall()

            return [
                {"date": row[0].isoformat() if row[0] else None, "count": row[1]}
                for row in rows
            ]
        except Exception as e:
            logger.error(f"Error in group_by: {e}")
            return []

    async def get_tags(self, filters: Optional[dict] = None) -> List[str]:
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
            where_clause, params = self._build_filter_sql(filters)
            where_sql = f"WHERE {where_clause}" if where_clause else ""

            query = text(
                f"""
                SELECT archive, count(*) as count
                FROM articles
                {where_sql}
                GROUP BY archive
                ORDER BY count DESC
            """
            )

            result = await self.session.execute(query, params)
            rows = result.fetchall()

            return [{"archive": row[0], "count": row[1]} for row in rows if row[0]]
        except Exception as e:
            logger.error(f"Error in get_archive_counts: {e}")
            return []
