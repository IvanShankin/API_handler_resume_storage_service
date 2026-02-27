from redis.asyncio import Redis

from src.database.models import Users
from src.repository.redis.base import BaseCache
from src.service.config.schemas import Config


class UserCacheRepository(BaseCache):

    def __init__(self, redis_session: Redis, config: Config):
        super().__init__(redis_session=redis_session, config=config)

    async def get_user(self, user_id: int) -> Users | None:
        return await super().get(
            key=f"storage:user:{user_id}",
            model_cls=Users
        )

    async def set_user(self, user: Users) -> None:
        await super().set(
            key=f"storage:user:{user.user_id}",
            time=self.conf.lifespan_redis.user,
            model_cls=user
        )
