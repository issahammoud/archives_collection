import os
from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "postgres")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "archives")
    DATABASE_HOST: str = os.getenv("DATABASE_HOST", "pgbouncer")
    DATABASE_PORT: int = int(os.getenv("DATABASE_PORT", "5432"))

    # Redis
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")

    # Embedding service
    EMBED_URL: str = os.getenv("EMBED_URL", "http://embedding:8000/v1/embeddings")

    # HNSW
    HNSW_EF_SEARCH: int = int(os.getenv("HNSW_EF_SEARCH", "100"))

    # API settings
    API_V1_PREFIX: str = "/api/v1"
    PROJECT_NAME: str = "Archives Collection"

    # Images path
    IMAGES_PATH: str = "/images"

    @property
    def async_database_url(self) -> str:
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.POSTGRES_DB}"

    @property
    def sync_database_url(self) -> str:
        return f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.POSTGRES_DB}"

    class Config:
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
