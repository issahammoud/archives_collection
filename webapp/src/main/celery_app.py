import os
from celery import Celery

celery_app = Celery(
    __name__,
    broker=os.environ["REDIS_URL"],
    backend=os.environ["REDIS_URL"],
    include=["src.utils.celery_tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    worker_enable_remote_control=True,
    task_create_missing_queues=True,
    # Important for long-running tasks
    worker_prefetch_multiplier=1,  # Process one task at a time
    task_acks_late=True,  # Acknowledge task after completion
    task_time_limit=None,  # No time limit
    broker_transport_options={"visibility_timeout": 43200},
    worker_hijack_root_logger=False,
    worker_redirect_stdouts=True,
    worker_redirect_stdouts_level="DEBUG",
)
