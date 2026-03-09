from typing import Dict
from sqlalchemy import text

from src.database.core import get_db
from src.exeptions.service_exc import DatabaseError, RedisError
from src.infrastructure.redis import get_redis
from src.service.utils.logger import get_logger


async def health_check_service() -> Dict:

    try:
        async for session_db in get_db():
            await session_db.execute(text("SELECT 1"))
    except Exception as e:
        get_logger().exception("Database connection failed")
        raise RedisError() from e


    try:
        async with get_redis() as redis_client:
            await redis_client.ping()
    except Exception as e:
        get_logger().error(f"Redis connection failed: {e}")
        raise DatabaseError() from e

    return {"status": "OK"}
