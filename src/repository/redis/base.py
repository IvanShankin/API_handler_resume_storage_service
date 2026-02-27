from typing import TypeVar, Type, List, Any

from orjson import orjson
from redis.asyncio import Redis

from src.database.base import Base
from src.service.config.schemas import Config


T = TypeVar("T", bound=Base)


class BaseCache:

    def __init__(self, redis_session: Redis, config: Config):
        self.redis_session = redis_session
        self.conf = config

    async def get(self, key: str, model_cls: Type[T], storage_list: bool = False) -> Any:
        """
        Если по указанному ключу имеются данные, то передаст каждый ключ как аргумент в `model_cls`
        :param key: Ключ по котору лежит словарь
        :param model_cls: Модель БД
        :param storage_list: True если хранит список
        :return: Если передан storage_list == True, то вернётся список из `model_cls`
        """
        result_redis = await self.redis_session.get(key)

        if result_redis:
            data = orjson.loads(result_redis)
            return [model_cls(**one_model) for one_model in data ]  if storage_list else model_cls(**data)

        return []  if storage_list else None

    async def set(self, key: str, time: int, model_cls: T | List[T]):
        """
        :param time: Время жизни в секундах
        :param model_cls: Экземпляр модели БД. Один экземпляр или список экземпляров
        """
        await self.redis_session.setex(
            key,
            time,
            orjson.dumps(model.to_dict() for model in model_cls)
            if model_cls is List else
            orjson.dumps(model_cls.to_dict()),
        )

    async def delete(self, keys: List[str]):
        async with self.redis_session.pipeline(transaction=False) as pipe:
            for key in keys:
                await pipe.delete(key)

            await pipe.execute()