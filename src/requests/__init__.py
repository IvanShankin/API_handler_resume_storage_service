from fastapi import APIRouter

from src.requests.get import router as get_router

main_router = APIRouter()

main_router.include_router(get_router)

__all__ = ['main_router']