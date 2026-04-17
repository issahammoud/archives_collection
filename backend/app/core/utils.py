import re
import base64
import hashlib
import logging
import httpx
import orjson
import requests
import numpy as np
from typing import List, Union
from functools import lru_cache
from wand.image import Image as WandImage
from google.oauth2 import service_account
from google.cloud import translate_v2 as translate

from app.core.config import settings
from app.core.enums import DBCOLUMNS
from app.core.s3_client import sync_s3_client, async_s3_client

logger = logging.getLogger(__name__)


def resize_image_from_bytes(
    image_bytes: bytes, target_height: int = 300
) -> bytes | None:
    """Resize image bytes and return resized bytes."""
    try:
        with WandImage(blob=image_bytes) as img:
            aspect_ratio = img.width / img.height
            new_width = int(target_height * aspect_ratio)
            img.resize(new_width, target_height)
            return img.make_blob(format="webp")
    except Exception as e:
        logger.error(f"Error resizing image: {e}")
        return None


def resize_image_for_html(
    image_data: str | bytes, target_height: int = 300
) -> str | None:
    """Resize image and return base64 encoded string for HTML display.

    Args:
        image_data: Either an S3 key (str) or raw image bytes
        target_height: Target height for the resized image
    """
    try:
        if isinstance(image_data, str):
            # It's an S3 key, download the image
            image_bytes = sync_s3_client.download_image(image_data)
            if image_bytes is None:
                return None
        else:
            image_bytes = image_data

        resized_bytes = resize_image_from_bytes(image_bytes, target_height)
        if resized_bytes is None:
            return None

        encoded_image = base64.b64encode(resized_bytes).decode()
        return f"data:image/webp;base64,{encoded_image}"

    except Exception as e:
        logger.error(f"Error processing image: {e}")
        return None


async def resize_image_for_html_async(
    s3_key: str, target_height: int = 300
) -> str | None:
    """Async version: Resize image from S3 and return base64 encoded string."""
    try:
        image_bytes = await async_s3_client.download_image(s3_key)
        if image_bytes is None:
            return None

        resized_bytes = resize_image_from_bytes(image_bytes, target_height)
        if resized_bytes is None:
            return None

        encoded_image = base64.b64encode(resized_bytes).decode()
        return f"data:image/webp;base64,{encoded_image}"

    except Exception as e:
        logger.error(f"Error processing image: {e}")
        return None


def process_image_to_webp(image_bytes: bytes, quality: int = 80) -> bytes | None:
    """Process image bytes to webp format."""
    if image_bytes is None:
        return None
    try:
        with WandImage(blob=image_bytes) as img:
            img.quality = quality
            img.format = "webp"
            return img.make_blob()
    except Exception as e:
        logger.error(f"Error converting image to webp: {e}")
        return image_bytes  # Return original if conversion fails


def save_image(s3_key: str, image_bytes: bytes, quality: int = 80) -> str | None:
    """Process and upload image to S3.

    Args:
        s3_key: The S3 key (path) where the image will be stored
        image_bytes: Raw image bytes to process and upload
        quality: WebP quality setting (0-100)

    Returns:
        The S3 key if successful, None otherwise
    """
    if image_bytes is None:
        return None

    processed_bytes = process_image_to_webp(image_bytes, quality)
    if processed_bytes is None:
        return None

    if sync_s3_client.upload_image(s3_key, processed_bytes):
        return s3_key
    return None


async def save_image_async(
    s3_key: str, image_bytes: bytes, quality: int = 80
) -> str | None:
    """Async version: Process and upload image to S3."""
    if image_bytes is None:
        return None

    processed_bytes = process_image_to_webp(image_bytes, quality)
    if processed_bytes is None:
        return None

    if await async_s3_client.upload_image(s3_key, processed_bytes):
        return s3_key
    return None


def get_image_path(date, section_url: str) -> str:
    """Generate S3 key for image based on date and URL hash.

    Args:
        date: Date object with year and month attributes
        section_url: URL to hash for unique filename

    Returns:
        S3 key in format: {year}/{month}/{hash}.webp
    """
    hash_url = hashlib.sha256(section_url.encode("utf-8")).hexdigest()
    try:
        year = date.year
        month = date.month
    except Exception:
        year = "unknown"
        month = "unknown"

    return f"{year}/{month}/{hash_url}.webp"


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
            resp = await client.post(settings.EMBED_URL, json=payload, timeout=timeout)
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


class JinaEmbedder:
    """
    Client léger utilisant requests pour interagir avec l'API vLLM.
    Configuré pour récupérer directement les vecteurs de 256 dimensions.
    """

    def __init__(
        self, 
        base_url: str = "http://embedding:8000/v1", 
        model: str = "jina"
    ):
        self.api_url = f"{base_url.rstrip('/')}/embeddings"
        self.model = model
        self.headers = {"Content-Type": "application/json"}

    def embed(self, text: Union[str, List[str]]) -> Union[List[float], List[List[float]]]:
        """
        Envoie une requête POST à vLLM et retourne les embeddings.
        """
        payload = {
            "model": self.model,
            "input": [text] if isinstance(text, str) else text,
            "encoding_format": "float"
        }

        try:
            response = requests.post(
                self.api_url, 
                headers=self.headers, 
                json=payload,
                timeout=30  # Timeout crucial pour les grosses requêtes
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Extraction des vecteurs
            # La structure vLLM/OpenAI est : {"data": [{"embedding": [...]}, ...]}
            embeddings = [item["embedding"] for item in data["data"]]

            return embeddings[0] if isinstance(text, str) else embeddings

        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur lors de la requête d'embedding : {e}")
            if response := getattr(e, 'response', None):
                logger.error(f"Détails de l'erreur : {response.text}")
            raise

    async def embed_async(self, text: Union[str, List[str]]) -> Union[List[float], List[List[float]]]:
        """Async version: send POST to vLLM and return embeddings."""
        payload = {
            "model": self.model,
            "input": [text] if isinstance(text, str) else text,
            "encoding_format": "float"
        }

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.api_url,
                    headers=self.headers,
                    json=payload,
                    timeout=30
                )
                response.raise_for_status()

                data = response.json()
                embeddings = [item["embedding"] for item in data["data"]]

                return embeddings[0] if isinstance(text, str) else embeddings

        except httpx.HTTPStatusError as e:
            logger.error(f"Erreur lors de la requête d'embedding async: {e}")
            if e.response:
                logger.error(f"Détails de l'erreur: {e.response.text}")
            raise


# Global embedder instance for query embedding
query_embedder = JinaEmbedder(base_url=settings.EMBED_URL.rsplit('/embeddings', 1)[0])


class SearchTranslator:
    def __init__(self):
        if settings.GOOGLE_CREDENTIALS_JSON:
            # Create credentials from the dictionary in settings
            creds = service_account.Credentials.from_service_account_info(
                settings.GOOGLE_CREDENTIALS_JSON
            )
            self.client = translate.Client(credentials=creds)
        else:
            self.client = translate.Client()

    @lru_cache(maxsize=1000)
    def to_french(self, text: str) -> str:
        if not text or len(text.strip()) < 2:
            return text
        try:
            result = self.client.translate(text, target_language='fr', format_='text')
            return result['translatedText']
        except Exception as e:
            # Log error and return original text as fallback
            logger.error(f"Translation API error: {e}")
            return text
        
translator = SearchTranslator()