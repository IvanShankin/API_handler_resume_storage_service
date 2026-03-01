from typing import TypeVar, Type, List, Any

from orjson import orjson
from redis.asyncio import Redis
from sqlalchemy import inspect

from src.database.base import Base
from src.service.config.schemas import Config


T = TypeVar("T", bound=Base)


class BaseCache:

    def __init__(self, redis_session: Redis, config: Config):
        self.redis_session = redis_session
        self.conf = config

    def _restore_enums(self, model_cls, data: dict) -> dict:
        mapper = inspect(model_cls)

        for column in mapper.columns:
            col_type = column.type

            if hasattr(col_type, "enum_class") and col_type.enum_class:
                enum_cls = col_type.enum_class
                field_name = column.name

                if field_name in data and data[field_name] is not None:
                    # если значение строка — конвертируем
                    if isinstance(data[field_name], str):
                        data[field_name] = enum_cls(data[field_name])

        return data

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

            if storage_list:
                return [
                    model_cls(**self._restore_enums(model_cls, one_model))
                    for one_model in data
                ]
            else:
                return model_cls(**self._restore_enums(model_cls, data))

        return [] if storage_list else None

    async def set(self, key: str, time: int, model_cls: T | List[T]):
        """
        :param time: Время жизни в секундах
        :param model_cls: Экземпляр модели БД. Один экземпляр или список экземпляров
        """
        if isinstance(model_cls, list):
            payload = [model.to_dict() for model in model_cls]
        else:
            payload = model_cls.to_dict()

        await self.redis_session.setex(
            key,
            time,
            orjson.dumps(payload),
        )

    async def delete(self, keys: List[str]):
        async with self.redis_session.pipeline(transaction=False) as pipe:
            for key in keys:
                await pipe.delete(key)

            await pipe.execute()