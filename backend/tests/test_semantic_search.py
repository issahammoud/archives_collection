"""
Unit tests for semantic search features.

These tests cover:
- fetch_articles_by_ids service method
- Semantic search API flow with session caching
- SearchMode routing logic

Note: Vector search (semantic_search_ids) requires pgvector which is not
available in SQLite, so we test the caching and pagination logic with mocks.
"""

import pytest
from datetime import date
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import AsyncClient

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.article_service import ArticleService
from app.core.enums import SearchMode
from tests.conftest import Article


class TestFetchArticlesByIds:
    """Tests for fetch_articles_by_ids service method."""

    @pytest.mark.asyncio
    async def test_fetch_by_ids_empty_list(self, async_session: AsyncSession):
        """Test fetching with empty ID list returns empty result."""
        service = ArticleService(async_session)
        articles = await service.fetch_articles_by_ids(article_ids=[])

        assert articles == []

    @pytest.mark.asyncio
    async def test_fetch_by_ids_basic(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test fetching articles by specific IDs."""
        service = ArticleService(async_session)

        # Get specific IDs from populated data
        target_ids = [1, 5, 10, 15, 20]
        articles = await service.fetch_articles_by_ids(article_ids=target_ids)

        assert len(articles) == 5
        returned_ids = [a["rowid"] for a in articles]
        assert set(returned_ids) == set(target_ids)

    @pytest.mark.asyncio
    async def test_fetch_by_ids_preserves_order(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test that fetch_articles_by_ids preserves input order."""
        service = ArticleService(async_session)

        # Specific order (not sorted)
        target_ids = [15, 3, 22, 8, 1]
        articles = await service.fetch_articles_by_ids(
            article_ids=target_ids,
            preserve_order=True,
        )

        returned_ids = [a["rowid"] for a in articles]
        assert returned_ids == target_ids

    @pytest.mark.asyncio
    async def test_fetch_by_ids_with_offset(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test offset-based pagination with fetch_articles_by_ids."""
        service = ArticleService(async_session)

        all_ids = list(range(1, 31))  # IDs 1-30

        # Page 1
        page1 = await service.fetch_articles_by_ids(
            article_ids=all_ids,
            offset=0,
            limit=10,
        )
        assert len(page1) == 10
        assert [a["rowid"] for a in page1] == list(range(1, 11))

        # Page 2
        page2 = await service.fetch_articles_by_ids(
            article_ids=all_ids,
            offset=10,
            limit=10,
        )
        assert len(page2) == 10
        assert [a["rowid"] for a in page2] == list(range(11, 21))

        # Page 3
        page3 = await service.fetch_articles_by_ids(
            article_ids=all_ids,
            offset=20,
            limit=10,
        )
        assert len(page3) == 10
        assert [a["rowid"] for a in page3] == list(range(21, 31))

    @pytest.mark.asyncio
    async def test_fetch_by_ids_offset_beyond_results(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test offset beyond available results returns empty."""
        service = ArticleService(async_session)

        target_ids = [1, 2, 3, 4, 5]
        articles = await service.fetch_articles_by_ids(
            article_ids=target_ids,
            offset=100,
            limit=10,
        )

        assert articles == []

    @pytest.mark.asyncio
    async def test_fetch_by_ids_partial_match(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test fetching with some non-existent IDs."""
        service = ArticleService(async_session)

        # Mix of existing and non-existing IDs
        target_ids = [1, 999, 5, 1000, 10]
        articles = await service.fetch_articles_by_ids(article_ids=target_ids)

        # Should only return existing articles
        assert len(articles) == 3
        returned_ids = {a["rowid"] for a in articles}
        assert returned_ids == {1, 5, 10}

    @pytest.mark.asyncio
    async def test_fetch_by_ids_article_structure(
        self, async_session: AsyncSession, populated_db: List[dict]
    ):
        """Test that fetched articles have correct structure."""
        service = ArticleService(async_session)

        articles = await service.fetch_articles_by_ids(article_ids=[1])

        assert len(articles) == 1
        article = articles[0]

        # Verify structure
        assert "rowid" in article
        assert "date" in article
        assert "archive" in article
        assert "title" in article
        assert "content" in article
        assert "tag" in article
        assert "link" in article


class TestGetCountForIds:
    """Tests for get_count_for_ids service method."""

    @pytest.mark.asyncio
    async def test_get_count_for_ids(self, async_session: AsyncSession):
        """Test that count returns length of ID list."""
        service = ArticleService(async_session)

        ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        count = await service.get_count_for_ids(ids)

        assert count == 10

    @pytest.mark.asyncio
    async def test_get_count_for_ids_empty(self, async_session: AsyncSession):
        """Test count with empty list."""
        service = ArticleService(async_session)

        count = await service.get_count_for_ids([])

        assert count == 0


class TestSearchModeRouting:
    """Tests for search mode determination logic."""

    def test_determine_search_mode_semantic(self):
        """Test semantic search mode detection when query provided."""
        from app.api.articles import determine_search_mode

        mode = determine_search_mode(query="test query")
        assert mode == SearchMode.SEMANTIC

    def test_determine_search_mode_browse(self):
        """Test browse mode detection when no query."""
        from app.api.articles import determine_search_mode

        mode = determine_search_mode(query=None)
        assert mode == SearchMode.BROWSE

    def test_determine_search_mode_browse_empty_query(self):
        """Test that empty string query results in browse mode."""
        from app.api.articles import determine_search_mode

        mode = determine_search_mode(query="")
        assert mode == SearchMode.BROWSE


class TestSemanticSearchAPIFlow:
    """Tests for semantic search API flow with session caching."""

    @pytest.mark.asyncio
    async def test_semantic_search_returns_session_id(
        self, client: AsyncClient, populated_db: List[dict]
    ):
        """Test that semantic search returns a search_session_id."""
        # Mock the embedding service and Redis cache
        with patch("app.api.articles.query_embedder") as mock_embedder, \
             patch("app.api.articles.search_session_cache") as mock_cache:

            # Setup mocks
            mock_embedder.embed_async = AsyncMock(return_value=[0.1] * 256)
            mock_cache.generate_session_id = MagicMock(return_value="test-session-123")
            mock_cache.store_search_results = AsyncMock(return_value=True)

            # Mock the service to return IDs
            with patch("app.api.articles.ArticleService") as mock_service_class:
                mock_service = MagicMock()
                mock_service.semantic_search_ids = AsyncMock(
                    return_value=([1, 2, 3, 4, 5], 5)
                )
                mock_service.fetch_articles_by_ids = AsyncMock(
                    return_value=[
                        {"rowid": i, "date": "2024-01-01", "archive": "lemonde",
                         "title": f"Article {i}", "content": "Content", "tag": "Test",
                         "link": f"http://test/{i}", "image": None}
                        for i in range(1, 6)
                    ]
                )
                mock_service_class.return_value = mock_service

                response = await client.get(
                    "/api/v1/articles",
                    params={
                        "query": "test semantic search",
                        "min_similarity": 0.7,
                    },
                )

                assert response.status_code == 200
                data = response.json()

                # Should have search_session_id for semantic search
                assert "search_session_id" in data
                assert data["search_session_id"] == "test-session-123"
                assert data["page"] == 1

    @pytest.mark.asyncio
    async def test_semantic_search_pagination_with_session(
        self, client: AsyncClient, populated_db: List[dict]
    ):
        """Test paginating semantic search results using session ID."""
        cached_data = {
            "article_ids": list(range(1, 101)),
            "total_count": 100,
            "query_hash": None,
        }

        with patch("app.api.articles.search_session_cache") as mock_cache:
            mock_cache.get_search_results = AsyncMock(return_value=cached_data)
            mock_cache.refresh_session_ttl = AsyncMock(return_value=True)

            with patch("app.api.articles.ArticleService") as mock_service_class:
                mock_service = MagicMock()
                mock_service.fetch_articles_by_ids = AsyncMock(
                    return_value=[
                        {"rowid": i, "date": "2024-01-01", "archive": "lemonde",
                         "title": f"Article {i}", "content": "Content", "tag": "Test",
                         "link": f"http://test/{i}", "image": None}
                        for i in range(11, 21)  # Page 2 articles
                    ]
                )
                mock_service_class.return_value = mock_service

                response = await client.get(
                    "/api/v1/articles",
                    params={
                        "search_session_id": "existing-session",
                        "page": 2,
                        "limit": 10,
                        # Need these for semantic mode detection
                        "query": "test",
                        "min_similarity": 0.7,
                    },
                )

                assert response.status_code == 200
                data = response.json()

                assert data["search_session_id"] == "existing-session"
                assert data["page"] == 2
                assert data["total_count"] == 100

                # Verify TTL was refreshed
                mock_cache.refresh_session_ttl.assert_called_once_with("existing-session")

    @pytest.mark.asyncio
    async def test_semantic_search_expired_session(
        self, client: AsyncClient, populated_db: List[dict]
    ):
        """Test handling of expired search session."""
        with patch("app.api.articles.search_session_cache") as mock_cache:
            mock_cache.get_search_results = AsyncMock(return_value=None)  # Expired

            response = await client.get(
                "/api/v1/articles",
                params={
                    "search_session_id": "expired-session",
                    "page": 2,
                    "query": "test",
                    "min_similarity": 0.7,
                },
            )

            assert response.status_code == 404
            assert "expired" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_semantic_search_empty_results(
        self, client: AsyncClient, populated_db: List[dict]
    ):
        """Test semantic search with no matching results."""
        with patch("app.api.articles.query_embedder") as mock_embedder:
            mock_embedder.embed_async = AsyncMock(return_value=[0.1] * 256)

            with patch("app.api.articles.ArticleService") as mock_service_class:
                mock_service = MagicMock()
                mock_service.semantic_search_ids = AsyncMock(return_value=([], 0))
                mock_service_class.return_value = mock_service

                response = await client.get(
                    "/api/v1/articles",
                    params={
                        "query": "nonexistent topic",
                        "min_similarity": 0.99,
                    },
                )

                assert response.status_code == 200
                data = response.json()

                assert data["articles"] == []
                assert data["total_count"] == 0
                assert data["search_session_id"] is None  # No session for empty results




class TestBrowseModeAPIFlow:
    """Tests for browse mode API flow (no query, direct DB)."""

    @pytest.mark.asyncio
    async def test_browse_mode_no_session_id(
        self, client: AsyncClient, populated_db: List[dict]
    ):
        """Test that browse mode does not return search_session_id."""
        response = await client.get(
            "/api/v1/articles",
            params={
                "date_start": "2024-01-01",
                "date_end": "2024-12-31",
                "limit": 10,
            },
        )

        assert response.status_code == 200
        data = response.json()

        assert data["search_session_id"] is None
        assert data["page"] is None
        assert data["last_seen"] is not None

    @pytest.mark.asyncio
    async def test_browse_mode_with_filters(
        self, client: AsyncClient, populated_db: List[dict]
    ):
        """Test browse mode with filters uses keyset pagination."""
        response = await client.get(
            "/api/v1/articles",
            params={
                "archives": "lemonde",
                "has_image": "true",
                "date_start": "2024-01-01",
                "date_end": "2024-12-31",
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Verify filters were applied
        for article in data["articles"]:
            assert article["archive"] == "lemonde"
            assert article["image"] is not None


class TestResponseStructureUpdates:
    """Tests for updated response structure."""

    @pytest.mark.asyncio
    async def test_response_includes_new_fields(
        self, client: AsyncClient, populated_db: List[dict]
    ):
        """Test that response includes new search_session_id and page fields."""
        response = await client.get(
            "/api/v1/articles",
            params={
                "date_start": "2024-01-01",
                "date_end": "2024-12-31",
            },
        )

        assert response.status_code == 200
        data = response.json()

        # These fields should always be present
        assert "articles" in data
        assert "total_count" in data
        assert "last_seen" in data
        assert "search_session_id" in data
        assert "page" in data

    @pytest.mark.asyncio
    async def test_semantic_response_structure(
        self, client: AsyncClient, populated_db: List[dict]
    ):
        """Test semantic search response has correct structure."""
        with patch("app.api.articles.query_embedder") as mock_embedder, \
             patch("app.api.articles.search_session_cache") as mock_cache:

            mock_embedder.embed_async = AsyncMock(return_value=[0.1] * 256)
            mock_cache.generate_session_id = MagicMock(return_value="session-123")
            mock_cache.store_search_results = AsyncMock(return_value=True)

            with patch("app.api.articles.ArticleService") as mock_service_class:
                mock_service = MagicMock()
                mock_service.semantic_search_ids = AsyncMock(return_value=([1], 1))
                mock_service.fetch_articles_by_ids = AsyncMock(
                    return_value=[{
                        "rowid": 1, "date": "2024-01-01", "archive": "lemonde",
                        "title": "Test", "content": "Content", "tag": "Test",
                        "link": "http://test", "image": None
                    }]
                )
                mock_service_class.return_value = mock_service

                response = await client.get(
                    "/api/v1/articles",
                    params={"query": "test", "min_similarity": 0.5},
                )

                assert response.status_code == 200
                data = response.json()

                # Semantic search specific
                assert data["search_session_id"] == "session-123"
                assert data["page"] == 1
                assert data["last_seen"] is None  # Not used for semantic


class TestEmbeddingServiceError:
    """Tests for embedding service error handling."""

    @pytest.mark.asyncio
    async def test_embedding_service_unavailable(
        self, client: AsyncClient, populated_db: List[dict]
    ):
        """Test handling of embedding service failures."""
        with patch("app.api.articles.query_embedder") as mock_embedder:
            mock_embedder.embed_async = AsyncMock(
                side_effect=Exception("Embedding service error")
            )

            response = await client.get(
                "/api/v1/articles",
                params={
                    "query": "test semantic",
                    "min_similarity": 0.7,
                },
            )

            assert response.status_code == 503
            assert "embedding" in response.json()["detail"].lower()
