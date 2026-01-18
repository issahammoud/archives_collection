import io
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import aioboto3
import boto3
from botocore.exceptions import ClientError

from app.core.config import settings

logger = logging.getLogger(__name__)


def get_s3_config() -> dict:
    """Get S3 configuration dictionary."""
    return {
        "endpoint_url": settings.S3_ENDPOINT_URL,
        "aws_access_key_id": settings.S3_ACCESS_KEY,
        "aws_secret_access_key": settings.S3_SECRET_KEY,
        "region_name": settings.S3_REGION,
    }


# Sync S3 Client (for Celery tasks)
class SyncS3Client:
    """Synchronous S3 client for use in Celery workers."""

    def __init__(self):
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client("s3", **get_s3_config())
        return self._client

    def upload_image(self, key: str, data: bytes, content_type: str = "image/webp") -> bool:
        """Upload image bytes to S3."""
        try:
            self.client.put_object(
                Bucket=settings.S3_BUCKET_NAME,
                Key=key,
                Body=data,
                ContentType=content_type,
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to upload {key} to S3: {e}")
            return False

    def download_image(self, key: str) -> bytes | None:
        """Download image from S3."""
        try:
            response = self.client.get_object(
                Bucket=settings.S3_BUCKET_NAME,
                Key=key,
            )
            return response["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.warning(f"Image not found in S3: {key}")
            else:
                logger.error(f"Failed to download {key} from S3: {e}")
            return None

    def check_exists(self, key: str) -> bool:
        """Check if an object exists in S3."""
        try:
            self.client.head_object(
                Bucket=settings.S3_BUCKET_NAME,
                Key=key,
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            logger.error(f"Failed to check existence of {key}: {e}")
            return False

    def get_presigned_url(self, key: str, expires_in: int = 3600) -> str | None:
        """Generate a presigned URL for an object."""
        try:
            url = self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": settings.S3_BUCKET_NAME, "Key": key},
                ExpiresIn=expires_in,
            )
            return url
        except ClientError as e:
            logger.error(f"Failed to generate presigned URL for {key}: {e}")
            return None


# Async S3 Client (for FastAPI endpoints)
class AsyncS3Client:
    """Asynchronous S3 client for use in FastAPI endpoints."""

    def __init__(self):
        self._session = aioboto3.Session()

    @asynccontextmanager
    async def get_client(self) -> AsyncGenerator:
        """Get an async S3 client context manager."""
        async with self._session.client("s3", **get_s3_config()) as client:
            yield client

    async def upload_image(self, key: str, data: bytes, content_type: str = "image/webp") -> bool:
        """Upload image bytes to S3."""
        try:
            async with self.get_client() as client:
                await client.put_object(
                    Bucket=settings.S3_BUCKET_NAME,
                    Key=key,
                    Body=data,
                    ContentType=content_type,
                )
            return True
        except ClientError as e:
            logger.error(f"Failed to upload {key} to S3: {e}")
            return False

    async def download_image(self, key: str) -> bytes | None:
        """Download image from S3."""
        try:
            async with self.get_client() as client:
                response = await client.get_object(
                    Bucket=settings.S3_BUCKET_NAME,
                    Key=key,
                )
                async with response["Body"] as stream:
                    return await stream.read()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.warning(f"Image not found in S3: {key}")
            else:
                logger.error(f"Failed to download {key} from S3: {e}")
            return None

    async def check_exists(self, key: str) -> bool:
        """Check if an object exists in S3."""
        try:
            async with self.get_client() as client:
                await client.head_object(
                    Bucket=settings.S3_BUCKET_NAME,
                    Key=key,
                )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            logger.error(f"Failed to check existence of {key}: {e}")
            return False

    async def get_presigned_url(self, key: str, expires_in: int = 3600) -> str | None:
        """Generate a presigned URL for an object."""
        try:
            async with self.get_client() as client:
                url = await client.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": settings.S3_BUCKET_NAME, "Key": key},
                    ExpiresIn=expires_in,
                )
            return url
        except ClientError as e:
            logger.error(f"Failed to generate presigned URL for {key}: {e}")
            return None


# Global instances
sync_s3_client = SyncS3Client()
async_s3_client = AsyncS3Client()
