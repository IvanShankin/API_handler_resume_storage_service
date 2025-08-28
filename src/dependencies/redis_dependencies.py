import os
from redis.asyncio import Redis  # Асинхронный клиент
from dotenv import load_dotenv
from fastapi.security import HTTPBearer

load_dotenv()
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = int(os.getenv('REDIS_PORT'))


security = HTTPBearer()

redis_client = Redis(
    host=REDIS_HOST,  # Хост Redis-сервера
    port=REDIS_PORT,  # Порт по умолчанию
    db=0,  # Номер базы данных (0-15)
    decode_responses=True  # Автоматическое декодирование из bytes в str
)

async def get_redis():
    try:
        yield redis_client
    finally:
        await redis_client.aclose()

class RedisWrapper:
    def __init__(self):
        self.redis = Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=0,
            decode_responses=True # автоматически преобразование с байт в строку
        )

    async def __aenter__(self):
        return self.redis

    async def __aexit__(self, exc_type, exc, tb):
        await self.redis.aclose()