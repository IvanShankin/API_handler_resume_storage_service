from sqlalchemy import  text
from sqlalchemy.ext.asyncio import AsyncSession

from fastapi import APIRouter, Depends, HTTPException

from srt.config import logger
from srt.database.database import get_db
from srt.dependencies.redis_dependencies import Redis, RedisWrapper

router = APIRouter()


# @router.get("/health")
# async def health_check(
#         db: AsyncSession = Depends(get_db),
#         redis: Redis = Depends(get_redis)
# ):
#     # Проверка БД
#     try:
#         await db.execute(text("SELECT 1"))
#     except Exception:
#         logger.error("Database connection failed")
#         raise HTTPException(500, "Database unavailable")
#
#     # Проверка Redis
#     try:
#         await redis.ping()
#     except Exception:
#         logger.error("Redis connection failed")
#         raise HTTPException(500, "Redis unavailable")
#
#     return {"status": "OK"}