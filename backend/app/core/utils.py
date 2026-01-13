import os
import re
import base64
import hashlib
import logging
import httpx
import orjson
import numpy as np
from wand.image import Image as WandImage

from app.core.config import settings
from app.core.enums import DBCOLUMNS

logger = logging.getLogger(__name__)


def resize_image_for_html(img_path: str, target_height: int = 300) -> str | None:
    """Resize image and return base64 encoded string for HTML display."""
    try:
        with WandImage(filename=img_path) as img:
            aspect_ratio = img.width / img.height
            new_width = int(target_height * aspect_ratio)
            img.resize(new_width, target_height)
            resized_image_bytes = img.make_blob(format="webp")

        encoded_image = base64.b64encode(resized_image_bytes).decode()
        return f"data:image/webp;base64,{encoded_image}"

    except Exception as e:
        logger.error(f"Error processing image: {e}")
        return None


def save_image(file_path: str, image_bytes: bytes, quality: int = 80) -> str | None:
    """Save image bytes to file path."""
    if image_bytes is not None:
        try:
            with WandImage(blob=image_bytes) as img:
                img.quality = quality
                img.format = "webp"
                img.save(filename=file_path)
        except Exception:
            with open(file_path, "wb") as file:
                file.write(image_bytes)
        finally:
            return file_path
    return None


def get_image_path(data_dir: str, date, section_url: str) -> str:
    """Generate image file path based on date and URL hash."""
    hash_url = hashlib.sha256(section_url.encode("utf-8")).hexdigest()
    try:
        year = date.year
        month = date.month
    except Exception:
        year = "unknown"
        month = "unknown"

    subdir = os.path.join(data_dir, str(year), str(month))
    os.makedirs(subdir, exist_ok=True)
    file_name = f"{hash_url}.webp"
    file_path = os.path.join(subdir, file_name)
    return file_path


def convert_count_to_str(count: int) -> str:
    """Convert large numbers to human-readable format."""
    if count >= 1000000:
        if not count % 1000000:
            return f"{count // 1000000}M"
        return f"{round(count / 1000000, 1)}M"

    if count >= 1000:
        if not count % 1000:
            return f"{count // 1000}K"
        return f"{round(count / 1000, 1)}K"

    return str(count)


def is_image_url(url: str) -> bool:
    """Check if URL points to an image file."""
    image_pattern = re.compile(
        r"\.(jpg|jpeg|png|gif|bmp|svg|webp|tiff)$", re.IGNORECASE
    )
    return bool(image_pattern.search(url))


async def get_embeddings_async(batch: list, timeout: int = 20) -> np.ndarray | None:
    """Get embeddings from embedding service asynchronously."""
    payload = prepare_payload(batch)

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                settings.EMBED_URL, json=payload, timeout=timeout
            )
            resp.raise_for_status()
            data = orjson.loads(resp.content)
            embeddings = np.array(data["embeddings"])
            if np.any(embeddings == None):
                return None
            return embeddings

    except Exception as e:
        logger.error(f"Error fetching embeddings for batch of size {len(batch)}: {e}")
        return None


def get_embeddings_sync(batch: list, timeout: int = 20) -> np.ndarray | None:
    """Get embeddings from embedding service synchronously (for Celery)."""
    import requests

    payload = prepare_payload(batch)

    try:
        resp = requests.post(settings.EMBED_URL, json=payload, timeout=timeout)
        resp.raise_for_status()
        data = orjson.loads(resp.content)
        embeddings = np.array(data["embeddings"])
        if np.any(embeddings == None):
            return None
        return embeddings

    except Exception as e:
        logger.error(f"Error fetching embeddings for batch of size {len(batch)}: {e}")
        return None


def prepare_payload(batch: list) -> dict:
    """Prepare payload for embedding service."""
    data = []

    for el in batch:
        text = ""
        title = el.get(DBCOLUMNS.title) or el.get("title")
        content = el.get(DBCOLUMNS.content) or el.get("content")
        tag = el.get(DBCOLUMNS.tag) or el.get("tag")

        text += f"title: {title}\n" if title else ""
        text += f"content: {content}\n" if content else ""
        text += f"topic: {tag}" if tag else ""
        data.append(text)

    return {"data": data}


async def get_query_embedding_async(query: str, timeout: int = 20) -> list | None:
    """Get embedding for a search query asynchronously."""
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                settings.EMBED_URL, json={"data": [query]}, timeout=timeout
            )
            resp.raise_for_status()
            data = orjson.loads(resp.content)
            embeddings = np.array(data["embeddings"])
            if np.any(embeddings == None):
                return None
            return embeddings.ravel().tolist()

    except Exception as e:
        logger.error(f"Error fetching embeddings for query {query}: {e}")
        return None


def get_query_embedding_sync(query: str, timeout: int = 20) -> list | None:
    """Get embedding for a search query synchronously (for Celery)."""
    import requests

    try:
        resp = requests.post(
            settings.EMBED_URL, json={"data": [query]}, timeout=timeout
        )
        resp.raise_for_status()
        data = orjson.loads(resp.content)
        embeddings = np.array(data["embeddings"])
        if np.any(embeddings == None):
            return None
        return embeddings.ravel().tolist()

    except Exception as e:
        logger.error(f"Error fetching embeddings for query {query}: {e}")
        return None
