import pytest
import asyncio
import os
import sys
from datetime import date, timedelta
from typing import AsyncGenerator, List
from unittest.mock import AsyncMock, MagicMock, patch
from types import ModuleType

from sqlalchemy import create_engine, event, Column, BigInteger, String, Date, Text, Index
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import StaticPool
from fastapi.testclient import TestClient
from httpx import AsyncClient, ASGITransport


# Use SQLite for testing (in-memory)
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

# Create a test-specific Base to avoid PostgreSQL-specific types
TestBase = declarative_base()


class Article(TestBase):
    """Test-specific Article model without PostgreSQL-specific columns."""
    __tablename__ = "articles"

    rowid = Column(BigInteger, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    archive = Column(String, nullable=False)
    image = Column(Text, nullable=True)
    title = Column(String, nullable=True)
    content = Column(String, nullable=True)
    tag = Column(String, nullable=True)
    link = Column(String, nullable=False)

    __table_args__ = (
        Index("articles_date_rowid_index", "date", "rowid"),
    )

    def to_dict(self):
        return {
            "rowid": self.rowid,
            "date": self.date.isoformat() if self.date else None,
            "archive": self.archive,
            "image": self.image,
            "title": self.title,
            "content": self.content,
            "tag": self.tag,
            "link": self.link,
        }


# Only mock during pytest runs (check if pytest is the main module)
_is_pytest = "pytest" in sys.modules and hasattr(sys, '_called_from_test')


def pytest_configure(config):
    """Called before test collection."""
    sys._called_from_test = True

    # Create a mock module for app.models.article BEFORE it gets imported
    _mock_article_module = ModuleType("app.models.article")
    _mock_article_module.Article = Article
    sys.modules["app.models.article"] = _mock_article_module


def pytest_unconfigure(config):
    """Called after all tests complete."""
    if hasattr(sys, '_called_from_test'):
        del sys._called_from_test


# Lazy import of app modules - only imported when fixtures are used
_app = None
_get_async_session = None


def _get_app():
    global _app, _get_async_session
    if _app is None:
        from app.core.database import get_async_session as gas
        from app.main import app as main_app
        _app = main_app
        _get_async_session = gas
    return _app, _get_async_session


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="function")
async def async_engine():
    """Create async engine for testing."""
    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        echo=False,
    )

    async with engine.begin() as conn:
        await conn.run_sync(TestBase.metadata.create_all)

    yield engine

    async with engine.begin() as conn:
        await conn.run_sync(TestBase.metadata.drop_all)

    await engine.dispose()


@pytest.fixture(scope="function")
async def async_session(async_engine) -> AsyncGenerator[AsyncSession, None]:
    """Create async session for testing."""
    async_session_maker = async_sessionmaker(
        bind=async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )

    async with async_session_maker() as session:
        yield session


@pytest.fixture(scope="function")
async def client(async_engine) -> AsyncGenerator[AsyncClient, None]:
    """Create test client with overridden database session."""
    app, get_async_session = _get_app()

    async_session_maker = async_sessionmaker(
        bind=async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )

    async def override_get_async_session():
        async with async_session_maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    app.dependency_overrides[get_async_session] = override_get_async_session

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test"
    ) as ac:
        yield ac

    app.dependency_overrides.clear()


@pytest.fixture
def sample_articles() -> List[dict]:
    """Generate sample article data for testing."""
    base_date = date(2024, 1, 1)
    articles = []

    for i in range(30):
        articles.append({
            "rowid": i + 1,
            "date": base_date + timedelta(days=i),
            "archive": ["lemonde", "lesechos", "leparisien"][i % 3],
            "image": f"/images/article_{i}.jpg" if i % 2 == 0 else None,
            "title": f"Test Article {i + 1}",
            "content": f"This is the content of test article {i + 1}. It contains some text for testing purposes.",
            "tag": ["Politics", "Economy", "Sports", "Technology"][i % 4],
            "link": f"https://example.com/article/{i + 1}",
        })

    return articles


@pytest.fixture
async def populated_db(async_session: AsyncSession, sample_articles: List[dict]):
    """Populate database with sample articles."""
    for article_data in sample_articles:
        article = Article(
            rowid=article_data["rowid"],
            date=article_data["date"],
            archive=article_data["archive"],
            image=article_data["image"],
            title=article_data["title"],
            content=article_data["content"],
            tag=article_data["tag"],
            link=article_data["link"],
        )
        async_session.add(article)

    await async_session.commit()
    return sample_articles


@pytest.fixture
def mock_embedding_response():
    """Mock embedding API response."""
    return [0.1] * 1024  # 1024-dimensional embedding
