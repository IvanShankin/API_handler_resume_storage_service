from logging import Logger
from typing import Dict

from fastapi import Depends
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.core import get_db
from src.exeptions.service_exc import DatabaseError, RedisError
from src.infrastructure.redis import get_redis
from src.service.utils.logger import get_logger


async def health_check_service(
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis),
    logger: Logger = Depends(get_logger)
) -> Dict:

    try:
        await db.execute(text("SELECT 1"))
    except Exception as e:
        logger.exception("Database connection failed")
        raise RedisError() from e


    try:
        await redis_client.ping()
    except Exception as e:
        get_logger(__name__).error(f"Redis connection failed: {e}")
        raise DatabaseError() from e

    return {"status": "OK"}
