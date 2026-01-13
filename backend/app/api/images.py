import os
import logging
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, Response

from app.core.config import settings
from app.core.utils import resize_image_for_html

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/raw/{path:path}")
async def get_raw_image(path: str):
    """Get the raw image file."""
    full_path = os.path.join(settings.IMAGES_PATH, path)

    if not os.path.exists(full_path):
        raise HTTPException(status_code=404, detail="Image not found")

    return FileResponse(
        path=full_path,
        media_type="image/webp",
    )


@router.get("/thumbnail/{path:path}")
async def get_thumbnail(path: str, height: int = 200):
    """Get a resized thumbnail of the image."""
    full_path = os.path.join(settings.IMAGES_PATH, path)

    if not os.path.exists(full_path):
        raise HTTPException(status_code=404, detail="Image not found")

    try:
        data_url = resize_image_for_html(full_path, target_height=height)
        if data_url is None:
            raise HTTPException(status_code=500, detail="Failed to process image")

        import base64

        base64_data = data_url.split(",")[1]
        image_bytes = base64.b64decode(base64_data)

        return Response(
            content=image_bytes,
            media_type="image/webp",
        )
    except Exception as e:
        logger.error(f"Error processing image: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/base64/{path:path}")
async def get_image_base64(path: str, height: int = 200):
    """Get the image as base64 encoded string."""
    full_path = os.path.join(settings.IMAGES_PATH, path)

    if not os.path.exists(full_path):
        return {"data": None}

    try:
        data_url = resize_image_for_html(full_path, target_height=height)
        return {"data": data_url}
    except Exception as e:
        logger.error(f"Error processing image: {e}")
        return {"data": None}
