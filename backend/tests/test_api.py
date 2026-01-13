"""
API endpoint tests for the backend.
"""
import pytest
from datetime import date
from typing import List
from httpx import AsyncClient

from tests.conftest import Article


class TestHealthEndpoint:
    """Tests for health check endpoint."""

    @pytest.mark.asyncio
    async def test_health_check(self, client: AsyncClient):
        """Test health check returns healthy status."""
        response = await client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}


class TestRootEndpoint:
    """Tests for root endpoint."""

    @pytest.mark.asyncio
    async def test_root(self, client: AsyncClient):
        """Test root endpoint returns welcome message."""
        response = await client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "docs" in data


class TestArticlesEndpoints:
    """Tests for articles API endpoints."""

    @pytest.mark.asyncio
    async def test_get_articles_empty_db(self, client: AsyncClient):
        """Test getting articles from empty database."""
        response = await client.get(
            "/api/v1/articles",
            params={
                "date_start": "2024-01-01",
                "date_end": "2024-12-31",
            }
        )
        assert response.status_code == 200
        data = response.json()
        assert data["articles"] == []
        assert data["total_count"] == 0

    @pytest.mark.asyncio
    async def test_get_articles_with_data(
        self, client: AsyncClient, populated_db: List[dict]
    ):
        """Test getting articles with populated database."""
        response = await client.get(
            "/api/v1/articles",
            params={
                "date_start": "2024-01-01",
                "date_end": "2024-12-31",
                "limit": 10,
            }
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["articles"]) == 10
        assert data["total_count"] == len(populated_db)

    @pytest.mark.asyncio
    async def test_get_articles_pagination(
        self, client: AsyncClient, populated_db: List[dict]
    ):
        """Test articles pagination via API."""
        # Get first page
        response1 = await client.get(
            "/api/v1/articles",
            params={
                "date_start": "2024-01-01",
                "date_end": "2024-12-31",
                "limit": 10,
                "desc_order": "true",
            }
        )
        assert response1.status_code == 200
        data1 = response1.json()
        assert len(data1["articles"]) == 10
        assert data1["last_seen"] is not None

        # Get second page using keyset
        last_seen = data1["last_seen"]["forward"]
        response2 = await client.get(
            "/api/v1/articles",
            params={
                "date_start": "2024-01-01",
                "date_end": "2024-12-31",
                "limit": 10,
                "desc_order": "true",
                "direction": "forward",
                "last_seen_date": last_seen["date"],
                "last_seen_rowid": last_seen["rowid"],
            }
        )
        assert response2.status_code == 200
        data2 = response2.json()
        assert len(data2["articles"]) == 10

        # Verify no overlap
        page1_ids = {a["rowid"] for a in data1["articles"]}
        page2_ids = {a["rowid"] for a in data2["articles"]}
        assert page1_ids.isdisjoint(page2_ids)

    @pytest.mark.asyncio
    async def test_get_articles_filter_by_archive(
        self, client: AsyncClient, populated_db: List[dict]
    ):
        """Test filtering articles by archive."""
        response = await client.get(
            "/api/v1/articles",
            params={
                "date_start": "2024-01-01",
                "date_end": "2024-12-31",
                "archives": "lemonde",
            }
        )
        assert response.status_code == 200
        data = response.json()

        for article in data["articles"]:
            assert article["archive"] == "lemonde"

    @pytest.mark.asyncio
    async def test_get_articles_filter_by_tag(
        self, client: AsyncClient, populated_db: List[dict]
    ):
        """Test filtering articles by tag."""
        response = await client.get(
            "/api/v1/articles",
            params={
                "date_start": "2024-01-01",
                "date_end": "2024-12-31",
                "tag": "Politics",
            }
        )
        assert response.status_code == 200
        data = response.json()

        for article in data["articles"]:
            assert "politics" in article["tag"].lower()

    @pytest.mark.asyncio
    async def test_get_articles_filter_has_image(
        self, client: AsyncClient, populated_db: List[dict]
    ):
        """Test filtering articles with images."""
        response = await client.get(
            "/api/v1/articles",
            params={
                "date_start": "2024-01-01",
                "date_end": "2024-12-31",
                "has_image": "true",
            }
        )
        assert response.status_code == 200
        data = response.json()

        for article in data["articles"]:
            assert article["image"] is not None

    @pytest.mark.asyncio
    async def test_get_article_count(
        self, client: AsyncClient, populated_db: List[dict]
    ):
        """Test getting article count."""
        response = await client.get("/api/v1/articles/count")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == len(populated_db)

    @pytest.mark.asyncio
    async def test_get_article_count_with_filter(
        self, client: AsyncClient, populated_db: List[dict]
    ):
        """Test getting article count with filter."""
        response = await client.get(
            "/api/v1/articles/count",
            params={"archives": "lemonde"}
        )
        assert response.status_code == 200
        data = response.json()

        expected_count = sum(
            1 for a in populated_db if a["archive"] == "lemonde"
        )
        assert data["count"] == expected_count

    @pytest.mark.asyncio
    async def test_get_date_range(
        self, client: AsyncClient, populated_db: List[dict]
    ):
        """Test getting date range."""
        response = await client.get("/api/v1/articles/date-range")
        assert response.status_code == 200
        data = response.json()

        expected_min = min(a["date"].isoformat() for a in populated_db)
        expected_max = max(a["date"].isoformat() for a in populated_db)

        assert data["min_date"] == expected_min
        assert data["max_date"] == expected_max

    @pytest.mark.asyncio
    async def test_get_tags(
        self, client: AsyncClient, populated_db: List[dict]
    ):
        """Test getting tags."""
        response = await client.get("/api/v1/articles/tags")
        assert response.status_code == 200
        data = response.json()

        assert "tags" in data
        assert len(data["tags"]) > 0

    @pytest.mark.asyncio
    async def test_get_archives(self, client: AsyncClient):
        """Test getting archives list."""
        response = await client.get("/api/v1/articles/archives")
        assert response.status_code == 200
        data = response.json()

        assert "archives" in data
        assert len(data["archives"]) > 0
        assert "lemonde" in data["archives"]

    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Requires PostgreSQL date_trunc function")
    async def test_get_group_by(
        self, client: AsyncClient, populated_db: List[dict]
    ):
        """Test group by endpoint."""
        response = await client.get(
            "/api/v1/articles/group-by",
            params={
                "date_start": "2024-01-01",
                "date_end": "2024-12-31",
                "group_by": "month",
            }
        )
        assert response.status_code == 200
        data = response.json()

        assert "data" in data
        total = sum(item["count"] for item in data["data"])
        assert total == len(populated_db)


class TestArticlesResponseStructure:
    """Tests for response structure validation."""

    @pytest.mark.asyncio
    async def test_article_response_structure(
        self, client: AsyncClient, populated_db: List[dict]
    ):
        """Test that article response has correct structure."""
        response = await client.get(
            "/api/v1/articles",
            params={
                "date_start": "2024-01-01",
                "date_end": "2024-12-31",
                "limit": 1,
            }
        )
        assert response.status_code == 200
        data = response.json()

        article = data["articles"][0]
        required_fields = ["rowid", "date", "archive", "title", "content", "tag", "link"]
        for field in required_fields:
            assert field in article, f"Missing field: {field}"

    @pytest.mark.asyncio
    async def test_last_seen_structure(
        self, client: AsyncClient, populated_db: List[dict]
    ):
        """Test that last_seen has correct structure."""
        response = await client.get(
            "/api/v1/articles",
            params={
                "date_start": "2024-01-01",
                "date_end": "2024-12-31",
                "limit": 10,
            }
        )
        assert response.status_code == 200
        data = response.json()

        last_seen = data["last_seen"]
        assert "forward" in last_seen
        assert "backward" in last_seen
        assert "date" in last_seen["forward"]
        assert "rowid" in last_seen["forward"]


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_invalid_limit(self, client: AsyncClient):
        """Test with invalid limit parameter."""
        response = await client.get(
            "/api/v1/articles",
            params={
                "date_start": "2024-01-01",
                "date_end": "2024-12-31",
                "limit": 0,
            }
        )
        assert response.status_code == 422  # Validation error

    @pytest.mark.asyncio
    async def test_limit_too_high(self, client: AsyncClient):
        """Test with limit exceeding maximum."""
        response = await client.get(
            "/api/v1/articles",
            params={
                "date_start": "2024-01-01",
                "date_end": "2024-12-31",
                "limit": 1000,
            }
        )
        assert response.status_code == 422  # Validation error

    @pytest.mark.asyncio
    async def test_invalid_direction(self, client: AsyncClient):
        """Test with invalid direction parameter."""
        response = await client.get(
            "/api/v1/articles",
            params={
                "date_start": "2024-01-01",
                "date_end": "2024-12-31",
                "direction": "invalid",
            }
        )
        assert response.status_code == 422  # Validation error

    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Requires PostgreSQL date_trunc function")
    async def test_invalid_group_by(self, client: AsyncClient):
        """Test with invalid group_by parameter."""
        response = await client.get(
            "/api/v1/articles/group-by",
            params={
                "date_start": "2024-01-01",
                "date_end": "2024-12-31",
                "group_by": "invalid",
            }
        )
        assert response.status_code == 422  # Validation error
