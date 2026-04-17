"""
Unit tests for SearchSessionCache (Redis client for search session caching).

These tests use a mock Redis client to test the caching logic without
requiring an actual Redis server.
"""

import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch

from app.core.redis_client import (
    SearchSessionCache,
    SEARCH_SESSION_TTL,
    SEARCH_SESSION_PREFIX,
)


class TestSearchSessionCacheBasic:
    """Basic tests for SearchSessionCache functionality."""

    def test_generate_session_id(self):
        """Test that session IDs are valid UUIDs."""
        cache = SearchSessionCache()
        session_id = cache.generate_session_id()

        assert session_id is not None
        assert len(session_id) == 36  # UUID4 format: 8-4-4-4-12
        assert session_id.count("-") == 4

    def test_generate_session_id_uniqueness(self):
        """Test that generated session IDs are unique."""
        cache = SearchSessionCache()
        session_ids = [cache.generate_session_id() for _ in range(100)]

        assert len(set(session_ids)) == 100, "All session IDs should be unique"

    def test_make_key(self):
        """Test Redis key generation."""
        cache = SearchSessionCache()
        session_id = "test-session-123"
        key = cache._make_key(session_id)

        assert key == f"{SEARCH_SESSION_PREFIX}{session_id}"
        assert key == "search:session:test-session-123"


class TestSearchSessionCacheStore:
    """Tests for storing search results in cache."""

    @pytest.mark.asyncio
    async def test_store_search_results_success(self):
        """Test successful storage of search results."""
        cache = SearchSessionCache()

        # Mock the Redis client
        mock_client = AsyncMock()
        mock_client.setex = AsyncMock(return_value=True)

        with patch.object(cache, "_get_client", return_value=mock_client):
            result = await cache.store_search_results(
                session_id="test-session",
                article_ids=[1, 2, 3, 4, 5],
                total_count=5,
                query_hash="abc123",
            )

        assert result is True
        mock_client.setex.assert_called_once()

        # Verify the call arguments
        call_args = mock_client.setex.call_args
        assert call_args[0][0] == "search:session:test-session"
        assert call_args[0][1] == SEARCH_SESSION_TTL

        # Verify the stored data
        stored_data = json.loads(call_args[0][2])
        assert stored_data["article_ids"] == [1, 2, 3, 4, 5]
        assert stored_data["total_count"] == 5
        assert stored_data["query_hash"] == "abc123"

    @pytest.mark.asyncio
    async def test_store_search_results_failure(self):
        """Test handling of storage failures."""
        cache = SearchSessionCache()

        mock_client = AsyncMock()
        mock_client.setex = AsyncMock(side_effect=Exception("Redis connection error"))

        with patch.object(cache, "_get_client", return_value=mock_client):
            result = await cache.store_search_results(
                session_id="test-session",
                article_ids=[1, 2, 3],
                total_count=3,
            )

        assert result is False

    @pytest.mark.asyncio
    async def test_store_large_result_set(self):
        """Test storing a large number of article IDs."""
        cache = SearchSessionCache()

        mock_client = AsyncMock()
        mock_client.setex = AsyncMock(return_value=True)

        large_ids = list(range(1, 10001))  # 10,000 IDs

        with patch.object(cache, "_get_client", return_value=mock_client):
            result = await cache.store_search_results(
                session_id="test-session",
                article_ids=large_ids,
                total_count=10000,
            )

        assert result is True

        # Verify the data is correctly serialized
        call_args = mock_client.setex.call_args
        stored_data = json.loads(call_args[0][2])
        assert len(stored_data["article_ids"]) == 10000


class TestSearchSessionCacheRetrieve:
    """Tests for retrieving search results from cache."""

    @pytest.mark.asyncio
    async def test_get_search_results_success(self):
        """Test successful retrieval of cached results."""
        cache = SearchSessionCache()

        stored_data = {
            "article_ids": [10, 20, 30, 40, 50],
            "total_count": 5,
            "query_hash": "xyz789",
        }

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=json.dumps(stored_data))

        with patch.object(cache, "_get_client", return_value=mock_client):
            result = await cache.get_search_results("test-session")

        assert result is not None
        assert result["article_ids"] == [10, 20, 30, 40, 50]
        assert result["total_count"] == 5
        assert result["query_hash"] == "xyz789"

    @pytest.mark.asyncio
    async def test_get_search_results_not_found(self):
        """Test retrieval when session doesn't exist."""
        cache = SearchSessionCache()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=None)

        with patch.object(cache, "_get_client", return_value=mock_client):
            result = await cache.get_search_results("nonexistent-session")

        assert result is None

    @pytest.mark.asyncio
    async def test_get_search_results_expired(self):
        """Test retrieval when session has expired (returns None)."""
        cache = SearchSessionCache()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=None)  # Expired key returns None

        with patch.object(cache, "_get_client", return_value=mock_client):
            result = await cache.get_search_results("expired-session")

        assert result is None

    @pytest.mark.asyncio
    async def test_get_search_results_failure(self):
        """Test handling of retrieval failures."""
        cache = SearchSessionCache()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=Exception("Redis connection error"))

        with patch.object(cache, "_get_client", return_value=mock_client):
            result = await cache.get_search_results("test-session")

        assert result is None


class TestSearchSessionCacheTTL:
    """Tests for TTL refresh functionality."""

    @pytest.mark.asyncio
    async def test_refresh_session_ttl_success(self):
        """Test successful TTL refresh."""
        cache = SearchSessionCache()

        mock_client = AsyncMock()
        mock_client.expire = AsyncMock(return_value=True)

        with patch.object(cache, "_get_client", return_value=mock_client):
            result = await cache.refresh_session_ttl("test-session")

        assert result is True
        mock_client.expire.assert_called_once_with(
            "search:session:test-session",
            SEARCH_SESSION_TTL,
        )

    @pytest.mark.asyncio
    async def test_refresh_session_ttl_not_found(self):
        """Test TTL refresh when session doesn't exist."""
        cache = SearchSessionCache()

        mock_client = AsyncMock()
        mock_client.expire = AsyncMock(return_value=False)  # Key doesn't exist

        with patch.object(cache, "_get_client", return_value=mock_client):
            result = await cache.refresh_session_ttl("nonexistent-session")

        assert result is False

    @pytest.mark.asyncio
    async def test_refresh_session_ttl_failure(self):
        """Test handling of TTL refresh failures."""
        cache = SearchSessionCache()

        mock_client = AsyncMock()
        mock_client.expire = AsyncMock(side_effect=Exception("Redis error"))

        with patch.object(cache, "_get_client", return_value=mock_client):
            result = await cache.refresh_session_ttl("test-session")

        assert result is False


class TestSearchSessionCacheDelete:
    """Tests for session deletion functionality."""

    @pytest.mark.asyncio
    async def test_delete_session_success(self):
        """Test successful session deletion."""
        cache = SearchSessionCache()

        mock_client = AsyncMock()
        mock_client.delete = AsyncMock(return_value=1)

        with patch.object(cache, "_get_client", return_value=mock_client):
            result = await cache.delete_session("test-session")

        assert result is True
        mock_client.delete.assert_called_once_with("search:session:test-session")

    @pytest.mark.asyncio
    async def test_delete_session_failure(self):
        """Test handling of deletion failures."""
        cache = SearchSessionCache()

        mock_client = AsyncMock()
        mock_client.delete = AsyncMock(side_effect=Exception("Redis error"))

        with patch.object(cache, "_get_client", return_value=mock_client):
            result = await cache.delete_session("test-session")

        assert result is False


class TestSearchSessionCacheIntegration:
    """Integration-style tests for the full cache workflow."""

    @pytest.mark.asyncio
    async def test_full_cache_workflow(self):
        """Test the complete store -> retrieve -> refresh -> delete workflow."""
        cache = SearchSessionCache()

        # Mock Redis client with state
        stored_data = {}

        async def mock_setex(key, ttl, value):
            stored_data[key] = value
            return True

        async def mock_get(key):
            return stored_data.get(key)

        async def mock_expire(key, ttl):
            return key in stored_data

        async def mock_delete(key):
            if key in stored_data:
                del stored_data[key]
                return 1
            return 0

        mock_client = AsyncMock()
        mock_client.setex = mock_setex
        mock_client.get = mock_get
        mock_client.expire = mock_expire
        mock_client.delete = mock_delete

        with patch.object(cache, "_get_client", return_value=mock_client):
            session_id = cache.generate_session_id()
            article_ids = [100, 200, 300, 400, 500]

            # Store
            store_result = await cache.store_search_results(
                session_id=session_id,
                article_ids=article_ids,
                total_count=5,
            )
            assert store_result is True

            # Retrieve
            retrieved = await cache.get_search_results(session_id)
            assert retrieved is not None
            assert retrieved["article_ids"] == article_ids
            assert retrieved["total_count"] == 5

            # Refresh TTL
            refresh_result = await cache.refresh_session_ttl(session_id)
            assert refresh_result is True

            # Delete
            delete_result = await cache.delete_session(session_id)
            assert delete_result is True

            # Verify deleted
            after_delete = await cache.get_search_results(session_id)
            assert after_delete is None

    @pytest.mark.asyncio
    async def test_pagination_simulation(self):
        """Test simulating pagination workflow with cached IDs."""
        cache = SearchSessionCache()

        # Simulate a search result with 100 articles
        all_ids = list(range(1, 101))
        stored_data = {}

        async def mock_setex(key, ttl, value):
            stored_data[key] = value
            return True

        async def mock_get(key):
            return stored_data.get(key)

        async def mock_expire(key, ttl):
            return key in stored_data

        mock_client = AsyncMock()
        mock_client.setex = mock_setex
        mock_client.get = mock_get
        mock_client.expire = mock_expire

        with patch.object(cache, "_get_client", return_value=mock_client):
            session_id = cache.generate_session_id()

            # Store all IDs
            await cache.store_search_results(
                session_id=session_id,
                article_ids=all_ids,
                total_count=100,
            )

            # Simulate multiple page requests
            page_size = 10
            for page_num in range(1, 11):
                result = await cache.get_search_results(session_id)
                assert result is not None

                # Simulate pagination (this would be done in the service)
                offset = (page_num - 1) * page_size
                page_ids = result["article_ids"][offset : offset + page_size]

                assert len(page_ids) == 10
                assert page_ids == list(range(offset + 1, offset + 11))

                # Refresh TTL on each access
                await cache.refresh_session_ttl(session_id)
