"""
Redis client for search session caching.

Implements the Snapshot Pattern for semantic search results:
- Caches article IDs from expensive vector searches
- Enables fast pagination without recomputing embeddings
- Uses TTL to automatically expire stale sessions
"""

import json
import logging
import uuid
from typing import Optional, List

import redis.asyncio as redis

from app.core.config import settings

logger = logging.getLogger(__name__)

# Session TTL: 15 minutes (sufficient for user browsing sessions)
SEARCH_SESSION_TTL = 900

# Redis key prefix for search sessions
SEARCH_SESSION_PREFIX = "search:session:"


class SearchSessionCache:
    """
    Async Redis client for caching semantic search results.

    The Snapshot Pattern:
    1. First request: Compute embeddings -> Vector Search -> Cache IDs -> Return page 1
    2. Subsequent pages: Retrieve cached IDs -> Paginate with DB lookup
    """

    def __init__(self):
        self._pool: Optional[redis.ConnectionPool] = None
        self._client: Optional[redis.Redis] = None

    async def _get_client(self) -> redis.Redis:
        """Lazy initialization of Redis connection pool."""
        if self._client is None:
            self._pool = redis.ConnectionPool.from_url(
                settings.REDIS_URL,
                decode_responses=True,
                max_connections=10,
            )
            self._client = redis.Redis(connection_pool=self._pool)
        return self._client

    def generate_session_id(self) -> str:
        """Generate a unique session ID for a new search."""
        return str(uuid.uuid4())

    def _make_key(self, session_id: str) -> str:
        """Create Redis key for a session."""
        return f"{SEARCH_SESSION_PREFIX}{session_id}"

    async def store_search_results(
        self,
        session_id: str,
        article_ids: List[int],
        total_count: int,
        similarities: Optional[dict] = None,
        query_hash: Optional[str] = None,
    ) -> bool:
        """
        Store semantic search results in Redis.

        Args:
            session_id: Unique identifier for this search session
            article_ids: List of article rowids from vector search (ordered by relevance)
            total_count: Total matching articles count
            similarities: Dict mapping rowid to similarity score
            query_hash: Optional hash of query params for validation

        Returns:
            True if stored successfully, False otherwise
        """
        try:
            client = await self._get_client()
            key = self._make_key(session_id)

            # Convert int keys to strings for JSON serialization
            sim_dict = {str(k): v for k, v in similarities.items()} if similarities else {}

            data = {
                "article_ids": article_ids,
                "total_count": total_count,
                "similarities": sim_dict,
                "query_hash": query_hash,
            }

            await client.setex(
                key,
                SEARCH_SESSION_TTL,
                json.dumps(data),
            )

            logger.debug(
                f"Stored search session {session_id}: {len(article_ids)} IDs, "
                f"total_count={total_count}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to store search session {session_id}: {e}")
            return False

    async def get_search_results(
        self,
        session_id: str,
    ) -> Optional[dict]:
        """
        Retrieve cached search results from Redis.

        Args:
            session_id: The session ID to retrieve

        Returns:
            Dict with article_ids, total_count, query_hash or None if not found/expired
        """
        try:
            client = await self._get_client()
            key = self._make_key(session_id)

            data = await client.get(key)

            if data is None:
                logger.debug(f"Search session {session_id} not found or expired")
                return None

            result = json.loads(data)
            logger.debug(
                f"Retrieved search session {session_id}: {len(result['article_ids'])} IDs"
            )
            return result

        except Exception as e:
            logger.error(f"Failed to retrieve search session {session_id}: {e}")
            return None

    async def refresh_session_ttl(self, session_id: str) -> bool:
        """
        Refresh the TTL of a session (called on pagination).

        Args:
            session_id: The session ID to refresh

        Returns:
            True if refreshed, False if session doesn't exist
        """
        try:
            client = await self._get_client()
            key = self._make_key(session_id)

            result = await client.expire(key, SEARCH_SESSION_TTL)
            return result

        except Exception as e:
            logger.error(f"Failed to refresh session TTL {session_id}: {e}")
            return False

    async def delete_session(self, session_id: str) -> bool:
        """
        Manually delete a search session.

        Args:
            session_id: The session ID to delete

        Returns:
            True if deleted, False otherwise
        """
        try:
            client = await self._get_client()
            key = self._make_key(session_id)

            await client.delete(key)
            logger.debug(f"Deleted search session {session_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to delete search session {session_id}: {e}")
            return False

    async def close(self):
        """Close the Redis connection pool."""
        if self._client:
            await self._client.close()
            self._client = None
        if self._pool:
            await self._pool.disconnect()
            self._pool = None


# Global singleton instance
search_session_cache = SearchSessionCache()
