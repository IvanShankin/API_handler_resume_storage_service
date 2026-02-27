from typing import List

from redis.asyncio import Redis

from src.database.models import Processing
from src.repository.redis.base import BaseCache
from src.service.config.schemas import Config


class ProcessingCacheRepository(BaseCache):

    def __init__(self, redis_session: Redis, config: Config):
        super().__init__(redis_session=redis_session, config=config)

    async def get_by_resume(self, resume_id: int) -> Processing | None:
        return await super().get(
            key=f"storage:processing_by_resume:{resume_id}",
            model_cls=Processing
        )

    async def set_by_resume(self, processing: Processing) -> None:
        await super().set(
            key=f"storage:processing_by_resume:{processing.resume_id}",
            time=self.conf.lifespan_redis.processing_by_resume,
            model_cls=processing
        )

    async def delete_by_resume(self, resume_ids: List[int]) -> None:
        await super().delete(keys=[f"storage:processing_by_resume:{resume_id}" for resume_id in resume_ids])

