import base64
import logging
from functools import lru_cache

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response

from app.core.s3_client import async_s3_client
from app.core.utils import resize_image_from_bytes

router = APIRouter()
logger = logging.getLogger(__name__)


# Simple in-memory LRU cache for thumbnails
@lru_cache(maxsize=1000)
def _get_cached_thumbnail(s3_key: str, height: int, image_hash: str) -> bytes | None:
    """Cache wrapper - image_hash ensures cache invalidation if image changes."""
    return None  # Placeholder, actual caching happens in the endpoint


# In-memory cache for raw images and thumbnails
_image_cache: dict[str, bytes] = {}
_MAX_CACHE_SIZE = 500


def _cache_image(key: str, data: bytes) -> None:
    """Add image to cache with simple size limit."""
    if len(_image_cache) >= _MAX_CACHE_SIZE:
        # Remove oldest entry (first item)
        oldest_key = next(iter(_image_cache))
        del _image_cache[oldest_key]
    _image_cache[key] = data


def _get_cached_image(key: str) -> bytes | None:
    """Get image from cache if available."""
    return _image_cache.get(key)


@router.get("/raw/{path:path}")
async def get_raw_image(path: str):
    """Get the raw image file from S3."""
    # Check cache first
    cache_key = f"raw:{path}"
    cached = _get_cached_image(cache_key)
    if cached:
        return Response(content=cached, media_type="image/webp")

    # Download from S3
    image_bytes = await async_s3_client.download_image(path)

    if image_bytes is None:
        raise HTTPException(status_code=404, detail="Image not found")

    # Cache and return
    _cache_image(cache_key, image_bytes)
    return Response(content=image_bytes, media_type="image/webp")


@router.get("/thumbnail/{path:path}")
async def get_thumbnail(path: str, height: int = 200):
    """Get a resized thumbnail of the image from S3."""
    # Check cache first
    cache_key = f"thumb:{path}:{height}"
    cached = _get_cached_image(cache_key)
    if cached:
        return Response(content=cached, media_type="image/webp")

    # Download from S3
    image_bytes = await async_s3_client.download_image(path)

    if image_bytes is None:
        raise HTTPException(status_code=404, detail="Image not found")

    try:
        resized_bytes = resize_image_from_bytes(image_bytes, target_height=height)
        if resized_bytes is None:
            raise HTTPException(status_code=500, detail="Failed to process image")

        # Cache and return
        _cache_image(cache_key, resized_bytes)
        return Response(content=resized_bytes, media_type="image/webp")

    except Exception as e:
        logger.error(f"Error processing image: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/base64/{path:path}")
async def get_image_base64(path: str, height: int = 200):
    """Get the image as base64 encoded string from S3."""
    # Check cache first
    cache_key = f"thumb:{path}:{height}"
    cached = _get_cached_image(cache_key)
    if cached:
        encoded = base64.b64encode(cached).decode()
        return {"data": f"data:image/webp;base64,{encoded}"}

    # Download from S3
    image_bytes = await async_s3_client.download_image(path)

    if image_bytes is None:
        return {"data": None}

    try:
        resized_bytes = resize_image_from_bytes(image_bytes, target_height=height)
        if resized_bytes is None:
            return {"data": None}

        # Cache the resized bytes
        _cache_image(cache_key, resized_bytes)

        encoded = base64.b64encode(resized_bytes).decode()
        return {"data": f"data:image/webp;base64,{encoded}"}

    except Exception as e:
        logger.error(f"Error processing image: {e}")
        return {"data": None}
