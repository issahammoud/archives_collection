from fastapi import APIRouter
from app.api import articles, tasks, images

api_router = APIRouter()

api_router.include_router(articles.router, prefix="/articles", tags=["articles"])
api_router.include_router(tasks.router, prefix="/tasks", tags=["tasks"])
api_router.include_router(images.router, prefix="/images", tags=["images"])
