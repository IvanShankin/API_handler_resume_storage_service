from typing import List

from redis.asyncio import Redis

from src.database.models import Resumes
from src.repository.redis.base import BaseCache
from src.service.config.schemas import Config


class ResumeCacheRepository(BaseCache):

    def __init__(self, redis_session: Redis, config: Config):
        super().__init__(redis_session=redis_session, config=config)

    async def get_by_requirement(self, requirement_id: int) -> List[Resumes] | None:
        return await super().get(
            key=f"resumes_by_requirement:{requirement_id}",
            model_cls=Resumes,
            storage_list=True
        )

    async def set_by_requirement(self, requirement_id: int, resumes: List[Resumes]) -> None:
        await super().set(
            key=f"resumes_by_requirement:{requirement_id}",
            time=self.conf.lifespan_redis.resume_by_requirement,
            model_cls=resumes
        )

    async def delete_by_requirement(self, requirement_id: int) -> None:
        await super().delete(keys=[f"resumes_by_requirement:{requirement_id}"])