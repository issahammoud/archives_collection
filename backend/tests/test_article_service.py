"""
Unit tests for ArticleService, focusing on keyset pagination.
"""
import pytest
from datetime import date, timedelta
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.article_service import ArticleService
from app.core.enums import DBCOLUMNS, OPERATORS
from tests.conftest import Article


class TestArticleServiceBasic:
    """Basic tests for ArticleService methods."""

    @pytest.mark.asyncio
    async def test_get_total_count_empty_db(self, async_session: AsyncSession):
        """Test total count on empty database."""
        service = ArticleService(async_session)
        count = await service.get_total_count()
        assert count == 0

    @pytest.mark.asyncio
    async def test_get_total_count_with_data(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test total count with populated database."""
        service = ArticleService(async_session)
        count = await service.get_total_count()
        assert count == len(populated_db)

    @pytest.mark.asyncio
    async def test_get_min_max_dates_empty_db(self, async_session: AsyncSession):
        """Test min/max dates on empty database."""
        service = ArticleService(async_session)
        min_date, max_date = await service.get_min_max_dates()
        assert min_date is None
        assert max_date is None

    @pytest.mark.asyncio
    async def test_get_min_max_dates_with_data(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test min/max dates with populated database."""
        service = ArticleService(async_session)
        min_date, max_date = await service.get_min_max_dates()

        expected_min = min(a["date"] for a in populated_db)
        expected_max = max(a["date"] for a in populated_db)

        assert min_date == expected_min
        assert max_date == expected_max

    @pytest.mark.asyncio
    async def test_get_tags(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test getting tags from database."""
        service = ArticleService(async_session)
        tags = await service.get_tags()

        assert len(tags) > 0
        # Tags should be title-cased
        for tag in tags:
            if tag:
                assert tag[0].isupper()


class TestKeysetPagination:
    """Tests specifically for keyset pagination algorithm."""

    @pytest.mark.asyncio
    async def test_fetch_first_page_desc(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test fetching first page in descending order."""
        service = ArticleService(async_session)
        articles = await service.fetch_data_keyset(
            limit=10,
            direction="forward",
            desc_order=True,
        )

        assert len(articles) == 10
        # Should be sorted by date desc, then rowid desc
        for i in range(len(articles) - 1):
            current = (articles[i]["date"], articles[i]["rowid"])
            next_item = (articles[i + 1]["date"], articles[i + 1]["rowid"])
            assert current >= next_item, "Articles should be in descending order"

    @pytest.mark.asyncio
    async def test_fetch_first_page_asc(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test fetching first page in ascending order."""
        service = ArticleService(async_session)
        articles = await service.fetch_data_keyset(
            limit=10,
            direction="forward",
            desc_order=False,
        )

        assert len(articles) == 10
        # Should be sorted by date asc, then rowid asc
        for i in range(len(articles) - 1):
            current = (articles[i]["date"], articles[i]["rowid"])
            next_item = (articles[i + 1]["date"], articles[i + 1]["rowid"])
            assert current <= next_item, "Articles should be in ascending order"

    @pytest.mark.asyncio
    async def test_keyset_pagination_forward_desc(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test keyset pagination moving forward in descending order."""
        service = ArticleService(async_session)

        # Fetch first page
        page1 = await service.fetch_data_keyset(
            limit=10,
            direction="forward",
            desc_order=True,
        )

        assert len(page1) == 10
        last_article = page1[-1]

        # Fetch second page using keyset
        page2 = await service.fetch_data_keyset(
            limit=10,
            direction="forward",
            desc_order=True,
            last_seen_date=date.fromisoformat(last_article["date"]),
            last_seen_rowid=last_article["rowid"],
        )

        assert len(page2) == 10

        # Verify no overlap between pages
        page1_ids = {a["rowid"] for a in page1}
        page2_ids = {a["rowid"] for a in page2}
        assert page1_ids.isdisjoint(page2_ids), "Pages should not have overlapping articles"

        # Verify page2 comes after page1 (in desc order, so earlier dates)
        last_of_page1 = (page1[-1]["date"], page1[-1]["rowid"])
        first_of_page2 = (page2[0]["date"], page2[0]["rowid"])
        assert last_of_page1 > first_of_page2, "Page 2 should contain earlier items"

    @pytest.mark.asyncio
    async def test_keyset_pagination_forward_asc(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test keyset pagination moving forward in ascending order."""
        service = ArticleService(async_session)

        # Fetch first page
        page1 = await service.fetch_data_keyset(
            limit=10,
            direction="forward",
            desc_order=False,
        )

        assert len(page1) == 10
        last_article = page1[-1]

        # Fetch second page using keyset
        page2 = await service.fetch_data_keyset(
            limit=10,
            direction="forward",
            desc_order=False,
            last_seen_date=date.fromisoformat(last_article["date"]),
            last_seen_rowid=last_article["rowid"],
        )

        assert len(page2) == 10

        # Verify no overlap
        page1_ids = {a["rowid"] for a in page1}
        page2_ids = {a["rowid"] for a in page2}
        assert page1_ids.isdisjoint(page2_ids)

        # Verify page2 comes after page1 (in asc order, so later dates)
        last_of_page1 = (page1[-1]["date"], page1[-1]["rowid"])
        first_of_page2 = (page2[0]["date"], page2[0]["rowid"])
        assert last_of_page1 < first_of_page2, "Page 2 should contain later items"

    @pytest.mark.asyncio
    async def test_keyset_pagination_backward_desc(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test keyset pagination moving backward in descending order."""
        service = ArticleService(async_session)

        # First get to page 2
        page1 = await service.fetch_data_keyset(
            limit=10,
            direction="forward",
            desc_order=True,
        )
        last_of_page1 = page1[-1]

        page2 = await service.fetch_data_keyset(
            limit=10,
            direction="forward",
            desc_order=True,
            last_seen_date=date.fromisoformat(last_of_page1["date"]),
            last_seen_rowid=last_of_page1["rowid"],
        )

        # Now go backward from page 2's first item
        first_of_page2 = page2[0]
        page_back = await service.fetch_data_keyset(
            limit=10,
            direction="backward",
            desc_order=True,
            last_seen_date=date.fromisoformat(first_of_page2["date"]),
            last_seen_rowid=first_of_page2["rowid"],
        )

        # The backward page should contain items from page 1
        page_back_ids = {a["rowid"] for a in page_back}
        page1_ids = {a["rowid"] for a in page1}

        # There should be overlap with page 1
        assert len(page_back_ids & page1_ids) > 0, "Backward navigation should return to previous items"

    @pytest.mark.asyncio
    async def test_keyset_pagination_complete_traversal(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test that keyset pagination can traverse all records without duplicates."""
        service = ArticleService(async_session)

        all_articles = []
        limit = 5
        last_seen_date = None
        last_seen_rowid = None

        # Traverse all pages
        for _ in range(10):  # Safety limit
            page = await service.fetch_data_keyset(
                limit=limit,
                direction="forward",
                desc_order=True,
                last_seen_date=last_seen_date,
                last_seen_rowid=last_seen_rowid,
            )

            if not page:
                break

            all_articles.extend(page)
            last_article = page[-1]
            last_seen_date = date.fromisoformat(last_article["date"])
            last_seen_rowid = last_article["rowid"]

        # Verify we got all articles
        assert len(all_articles) == len(populated_db)

        # Verify no duplicates
        rowids = [a["rowid"] for a in all_articles]
        assert len(rowids) == len(set(rowids)), "Should have no duplicate articles"

    @pytest.mark.asyncio
    async def test_keyset_pagination_with_same_dates(
        self, async_session: AsyncSession
    ):
        """Test keyset pagination when multiple articles have the same date."""
        # Create articles with same date
        same_date = date(2024, 6, 15)
        for i in range(10):
            article = Article(
                rowid=100 + i,
                date=same_date,
                archive="lemonde",
                title=f"Same Date Article {i}",
                content=f"Content {i}",
                tag="Test",
                link=f"https://example.com/same/{i}",
            )
            async_session.add(article)
        await async_session.commit()

        service = ArticleService(async_session)

        # Fetch first page
        page1 = await service.fetch_data_keyset(
            limit=5,
            direction="forward",
            desc_order=True,
        )

        assert len(page1) == 5
        last_article = page1[-1]

        # Fetch second page
        page2 = await service.fetch_data_keyset(
            limit=5,
            direction="forward",
            desc_order=True,
            last_seen_date=date.fromisoformat(last_article["date"]),
            last_seen_rowid=last_article["rowid"],
        )

        assert len(page2) == 5

        # Verify no overlap even with same dates
        page1_ids = {a["rowid"] for a in page1}
        page2_ids = {a["rowid"] for a in page2}
        assert page1_ids.isdisjoint(page2_ids), "Should handle same-date articles correctly"


class TestArticleServiceFilters:
    """Tests for filter functionality."""

    @pytest.mark.asyncio
    async def test_filter_by_date_range(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test filtering by date range."""
        service = ArticleService(async_session)

        start_date = date(2024, 1, 5)
        end_date = date(2024, 1, 15)

        filters = {
            DBCOLUMNS.date: [
                (OPERATORS.ge, start_date),
                (OPERATORS.le, end_date),
            ]
        }

        articles = await service.fetch_data_keyset(limit=30, filters=filters)

        for article in articles:
            article_date = date.fromisoformat(article["date"])
            assert start_date <= article_date <= end_date

    @pytest.mark.asyncio
    async def test_filter_by_archive(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test filtering by archive."""
        service = ArticleService(async_session)

        filters = {
            DBCOLUMNS.archive: [(OPERATORS.in_, ["lemonde"])]
        }

        articles = await service.fetch_data_keyset(limit=30, filters=filters)

        assert len(articles) > 0
        for article in articles:
            assert article["archive"] == "lemonde"

    @pytest.mark.asyncio
    async def test_filter_by_multiple_archives(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test filtering by multiple archives."""
        service = ArticleService(async_session)

        filters = {
            DBCOLUMNS.archive: [(OPERATORS.in_, ["lemonde", "lesechos"])]
        }

        articles = await service.fetch_data_keyset(limit=30, filters=filters)

        assert len(articles) > 0
        for article in articles:
            assert article["archive"] in ["lemonde", "lesechos"]

    @pytest.mark.asyncio
    async def test_filter_by_tag(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test filtering by tag."""
        service = ArticleService(async_session)

        filters = {
            DBCOLUMNS.tag: [(OPERATORS.like, "Politics")]
        }

        articles = await service.fetch_data_keyset(limit=30, filters=filters)

        assert len(articles) > 0
        for article in articles:
            assert "politics" in article["tag"].lower()

    @pytest.mark.asyncio
    async def test_filter_by_has_image(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test filtering by has image."""
        service = ArticleService(async_session)

        filters = {
            DBCOLUMNS.image: [(OPERATORS.notnull, None)]
        }

        articles = await service.fetch_data_keyset(limit=30, filters=filters)

        assert len(articles) > 0
        for article in articles:
            assert article["image"] is not None
            assert article["image"] != ""

    @pytest.mark.asyncio
    async def test_filter_count(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test that count respects filters."""
        service = ArticleService(async_session)

        filters = {
            DBCOLUMNS.archive: [(OPERATORS.in_, ["lemonde"])]
        }

        count = await service.get_total_count(filters=filters)
        articles = await service.fetch_data_keyset(limit=100, filters=filters)

        assert count == len(articles)

    @pytest.mark.asyncio
    async def test_combined_filters(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test combining multiple filters."""
        service = ArticleService(async_session)

        filters = {
            DBCOLUMNS.archive: [(OPERATORS.in_, ["lemonde"])],
            DBCOLUMNS.image: [(OPERATORS.notnull, None)],
        }

        articles = await service.fetch_data_keyset(limit=30, filters=filters)

        for article in articles:
            assert article["archive"] == "lemonde"
            assert article["image"] is not None


class TestGroupBy:
    """Tests for group by functionality.

    Note: These tests use PostgreSQL-specific date_trunc function,
    so they are skipped when running with SQLite.
    """

    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Requires PostgreSQL date_trunc function")
    async def test_group_by_day(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test grouping by day."""
        service = ArticleService(async_session)
        result = await service.group_by("day")

        assert len(result) > 0
        total_count = sum(item["count"] for item in result)
        assert total_count == len(populated_db)

    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Requires PostgreSQL date_trunc function")
    async def test_group_by_month(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test grouping by month."""
        service = ArticleService(async_session)
        result = await service.group_by("month")

        assert len(result) > 0
        total_count = sum(item["count"] for item in result)
        assert total_count == len(populated_db)

    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Requires PostgreSQL date_trunc function")
    async def test_group_by_year(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test grouping by year."""
        service = ArticleService(async_session)
        result = await service.group_by("year")

        assert len(result) > 0
        total_count = sum(item["count"] for item in result)
        assert total_count == len(populated_db)

    @pytest.mark.asyncio
    async def test_group_by_invalid_value(self, async_session: AsyncSession):
        """Test group by with invalid value raises error."""
        service = ArticleService(async_session)

        with pytest.raises(ValueError, match="must be 'day', 'month', or 'year'"):
            await service.group_by("invalid")
