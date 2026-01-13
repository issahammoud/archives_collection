import os
from celery import Celery

from app.core.config import settings

celery_app = Celery(
    "archives_collection",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=["app.tasks.celery_tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    task_time_limit=None,
    broker_transport_options={"visibility_timeout": 43200},
)
