from functools import lru_cache
from typing import Optional, Dict, Any
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # --- Database ---
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_DB: str = "archives"
    DATABASE_HOST: str = "pgbouncer"
    DATABASE_PORT: int = 5432

    # --- Redis ---
    REDIS_URL: str = "redis://redis:6379/0"

    # --- Embedding service (Jina v4 / Google) ---
    EMBED_URL: str = "http://embedding:8000/v1/embeddings"
    JINA_API_KEY: Optional[str] = None
    
    # --- Google Translation ---
    # Path to your JSON file
    GOOGLE_APPLICATION_CREDENTIALS: Optional[str] = None
    # OR: The full JSON content (handy for Docker/production)
    GOOGLE_CREDENTIALS_JSON: Optional[str] = None

    # --- HNSW ---
    HNSW_EF_SEARCH: int = 100

    # --- API settings ---
    API_V1_PREFIX: str = "/api/v1"
    PROJECT_NAME: str = "Archives Collection"

    # --- Storage ---
    IMAGES_PATH: str = "/images"
    S3_ENDPOINT_URL: str = ""
    S3_ACCESS_KEY: str = ""
    S3_SECRET_KEY: str = ""
    S3_BUCKET_NAME: str = "archives-images"
    S3_REGION: str = "gra"

    GOOGLE_CREDENTIALS_JSON: Optional[Dict[str, Any]] = None

    model_config = SettingsConfigDict(
        env_file=".env", 
        extra="ignore", 
        case_sensitive=True
    )

    @property
    def async_database_url(self) -> str:
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.POSTGRES_DB}"

    @property
    def sync_database_url(self) -> str:
        return f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.POSTGRES_DB}"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
