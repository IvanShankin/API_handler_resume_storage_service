from typing import List

from redis.asyncio import Redis

from src.database.models import Requirements
from src.repository.redis.base import BaseCache
from src.service.config.schemas import Config


class RequirementCacheRepository(BaseCache):

    def __init__(self, redis_session: Redis, config: Config):
        super().__init__(redis_session=redis_session, config=config)

    async def get_by_user(self, user_id: int) -> List[Requirements] | None:
        return await super().get(
            key=f"requirements_by_user:{user_id}",
            model_cls=Requirements,
            storage_list=True
        )

    async def set_by_user(self, user_id: int, requirements: List[Requirements]) -> None:
        await super().set(
            key=f"requirements_by_user:{user_id}",
            time=self.conf.lifespan_redis.resume_by_requirement,
            model_cls=requirements
        )

    async def delete_by_user(self, user_id: int) -> None:
        await super().delete(keys=[f"requirements_by_user:{user_id}"])