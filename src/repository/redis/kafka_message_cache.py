from redis.asyncio import Redis

from src.service.config.schemas import Config


class KafkaMessageCacheRepository:

    def __init__(self, redis_session: Redis, config: Config):
        self.redis_session = redis_session
        self.conf = config

    async def get(self, message_id: str) -> str | None:
        return await self.redis_session.get(
            f"storage:processed_message:{message_id}",
        )

    async def set(self, message_id: str) -> None:
        await self.redis_session.setex(
            name=f"storage:processed_message:{message_id}",
            time=self.conf.lifespan_redis.kafka_message,
            value="_"
        )