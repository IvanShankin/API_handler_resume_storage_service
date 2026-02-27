from fastapi import Depends
from redis.asyncio import Redis

from src.infrastructure.redis import get_redis
from src.repository.redis.kafka_message_cache import KafkaMessageCacheRepository
from src.repository.redis.processing_cache import ProcessingCacheRepository
from src.repository.redis.requirement_cache import RequirementCacheRepository
from src.repository.redis.resume_cache import ResumeCacheRepository
from src.repository.redis.user_cache import UserCacheRepository
from src.service.config import get_config
from src.service.config.schemas import Config


async def get_kafka_message_cache_repository(
    redis: Redis = Depends(get_redis),
    conf: Config = Depends(get_config),
) -> KafkaMessageCacheRepository:
    return KafkaMessageCacheRepository(redis, conf)


async def get_processing_cache_repository(
    redis: Redis = Depends(get_redis),
    conf: Config = Depends(get_config),
) -> ProcessingCacheRepository:
    return ProcessingCacheRepository(redis, conf)


async def get_requirement_cache_repository(
    redis: Redis = Depends(get_redis),
    conf: Config = Depends(get_config),
) -> RequirementCacheRepository:
    return RequirementCacheRepository(redis, conf)


async def get_resume_cache_repository(
    redis: Redis = Depends(get_redis),
    conf: Config = Depends(get_config),
) -> ResumeCacheRepository:
    return ResumeCacheRepository(redis, conf)


async def get_user_cache_repository(
    redis: Redis = Depends(get_redis),
    conf: Config = Depends(get_config),
) -> UserCacheRepository:
    return UserCacheRepository(redis, conf)

