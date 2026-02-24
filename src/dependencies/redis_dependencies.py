from redis.asyncio import Redis

from src.service.config import get_config


_redis_client: Redis | None = None


async def init_redis():
    global _redis_client

    conf = get_config()

    _redis_client = Redis(
        host=conf.env.redis_host,
        port=conf.env.redis_port,
        db=0,
        decode_responses=True,
    )


async def set_redis(redis: Redis):
    global _redis_client
    _redis_client = redis


async def close_redis():
    global _redis_client

    if _redis_client:
        await _redis_client.aclose()


def get_redis() -> Redis:
    if _redis_client is None:
        raise RuntimeError("Redis not initialized")

    return _redis_client