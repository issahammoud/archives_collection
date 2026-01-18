import logging
from typing import Optional, List
from datetime import date
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse

from app.core.config import settings
from app.core.enums import DBCOLUMNS, OPERATORS, CeleryTasks
from app.tasks.celery_tasks import collection_task, download_task, revoke_task
from app.schemas import (
    TaskResponse,
    TaskStatusResponse,
    CollectionRequest,
    DownloadRequest,
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/collection/start", response_model=TaskResponse)
async def start_collection(request: CollectionRequest):
    """Start a data collection task."""
    try:
        task = collection_task.apply_async(
            args=(
                request.archives,
                request.begin_date.isoformat(),
                request.end_date.isoformat(),
            )
        )

        return TaskResponse(
            task_id=str(task.id),
            status="started",
            task_name=str(CeleryTasks.collect),
        )
    except ValueError as e:
        logger.error(f"Validation error in collection task: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to start collection task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/collection/stop")
async def stop_collection(task_id: str):
    """Stop a running collection task."""
    try:
        revoke_task(task_id)
        return {"status": "stopped", "task_id": task_id}
    except Exception as e:
        logger.error(f"Failed to stop collection task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/collection/status/{task_id}", response_model=TaskStatusResponse)
async def get_collection_status(task_id: str):
    """Get the status of a collection task."""
    try:
        result = collection_task.AsyncResult(task_id)

        # Handle the result - convert exceptions to string
        task_result = None
        if result.ready():
            if result.successful():
                task_result = result.result
            else:
                # Task failed - convert exception to string
                task_result = str(result.result) if result.result else None

        return TaskStatusResponse(
            task_id=task_id,
            status=result.state,
            result=task_result,
        )
    except Exception as e:
        logger.error(f"Failed to get collection status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/download/start", response_model=TaskResponse)
async def start_download(
    archives: Optional[List[str]] = Query(None),
    tag: Optional[str] = None,
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
    query: Optional[str] = None,
    has_image: Optional[bool] = None,
    order: bool = True,
):
    """Start a data download task."""
    try:
        filters = {}

        if date_start or date_end:
            filters[DBCOLUMNS.date] = []
            if date_start:
                filters[DBCOLUMNS.date].append((OPERATORS.ge, date_start.isoformat()))
            if date_end:
                filters[DBCOLUMNS.date].append((OPERATORS.le, date_end.isoformat()))

        if tag:
            filters[DBCOLUMNS.tag] = [(OPERATORS.like, tag.strip())]

        if archives:
            filters[DBCOLUMNS.archive] = [(OPERATORS.in_, archives)]

        if query:
            filters[DBCOLUMNS.text_searchable] = [(OPERATORS.ts, query)]

        if has_image:
            filters[DBCOLUMNS.image] = [(OPERATORS.notnull, None)]

        columns = [
            DBCOLUMNS.rowid,
            DBCOLUMNS.date,
            DBCOLUMNS.archive,
            DBCOLUMNS.link,
            DBCOLUMNS.title,
            DBCOLUMNS.content,
            DBCOLUMNS.tag,
            DBCOLUMNS.image,
        ]

        task = download_task.apply_async(args=(columns, filters, order))

        return TaskResponse(
            task_id=task.id,
            status="started",
            task_name=CeleryTasks.download,
        )
    except Exception as e:
        logger.error(f"Failed to start download task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/download/status/{task_id}", response_model=TaskStatusResponse)
async def get_download_status(task_id: str):
    """Get the status of a download task."""
    try:
        result = download_task.AsyncResult(task_id)

        # Handle the result - convert exceptions to string
        task_result = None
        if result.ready():
            if result.successful():
                task_result = result.result
            else:
                # Task failed - convert exception to string
                task_result = str(result.result) if result.result else None

        return TaskStatusResponse(
            task_id=task_id,
            status=result.state,
            result=task_result,
        )
    except Exception as e:
        logger.error(f"Failed to get download status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/download/file")
async def download_file():
    """Download the generated data file."""
    import os

    zip_path = f"{settings.IMAGES_PATH}/data.zip"

    if not os.path.exists(zip_path):
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(
        path=zip_path,
        media_type="application/zip",
        filename="data.zip",
    )
