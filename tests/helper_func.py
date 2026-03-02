from datetime import datetime, UTC, timedelta
from typing import Type, Optional
from dateutil.parser import parse

from jose import jwt
from orjson import orjson

from src.service.config import get_config
from src.service.utils.logger import get_logger


class FakeAdminClient:
    def __init__(self):
        pass

    async def list_topics(self):
        conf = get_config()
        return [conf.env.topic_uploading_data]


class FakeKafkaMessage:
    def __init__(
        self,
        data: dict,
        topic: str = "test_topic",
        key: Optional[str] = None,
        partition: int = 0,
        offset: int = 0,
    ) -> None:
        self.topic: str = topic
        self.partition: int = partition
        self.offset: int = offset

        self.key: Optional[bytes] = (
            key.encode("utf-8") if key is not None else None
        )

        self.value: bytes = orjson.dumps(data)


def create_accesses_token(user_id: int) -> str:
    conf = get_config()

    to_encode = {"sub": str(user_id)}.copy()

    # Установка времени истечения токена
    expire = datetime.now(UTC) + timedelta(minutes=conf.tokens.access_token_expire_minutes)
    to_encode.update({"exp": expire})

    return jwt.encode(
        to_encode,
        conf.env.secret_key,
        algorithm=conf.tokens.algorithm
    )


def comparison_models(Expected: Type | dict, Actual: Type | dict, keys_not_checked: list = []):
    """Сравнивает две модели БД"""
    if not isinstance(Expected, dict):
        Expected: dict = Expected.to_dict()
    if not isinstance(Actual, dict):
        Actual: dict = Actual.to_dict()

    if not Actual:
        return False

    for key in Expected.keys():
        if not key in keys_not_checked:
            # если ожидаемый результат должен быть датой, и актуальный является не датой
            if isinstance(Expected[key], datetime) and not isinstance(Actual[key], datetime):
                assert Expected[key] == parse(Actual[key])
            elif isinstance(Expected[key], dict) and isinstance(Actual[key], dict):
                comparison_models(Expected[key], Actual[key])
            else:
                if not Expected[key] == Actual[key]:
                    get_logger(__name__).info(f"ключ '{key}' не совпал")
                    return False

        return True