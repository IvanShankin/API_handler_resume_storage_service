import fakeredis
import pytest_asyncio

from sqlalchemy import delete

from helper_func import FakeAdminClient
from src.api.app import init_fastapi_app
from src.database.creating import create_database
from src.database.models import Users, Resumes, Requirements, Processing
from src.infrastructure.kafka.admin_client import set_admin_client
from src.infrastructure.kafka.topic_manager import check_exists_topic
from src.infrastructure.redis import set_redis, get_redis
from src.service.config import init_config

# ФИКСТУРЫ НЕ УБИРАТЬ, ИСПОЛЬЗУЮТСЯ В ТЕСТАХ
from tests.help_fixtures import session_db, client_with_db
from tests.receiving_fixtures import user_service_fix, create_user, resume_service_fix, requirement_service_fix, \
    create_requirement, resume_service_fix, create_resume, processing_service_fix, create_processing, \
    kafka_event_handler_fix


@pytest_asyncio.fixture(scope='session', autouse=True)
async def start_test():
    conf = init_config()
    if conf.env.mode != "TEST":
        raise Exception("Используется основная БД!")

    await create_database()
    await set_redis(fakeredis.aioredis.FakeRedis())
    await set_admin_client(FakeAdminClient())
    await check_exists_topic(conf.env.topic_uploading_data)

    init_fastapi_app()


@pytest_asyncio.fixture(scope="function", autouse=True)
async def clearing_db(session_db):
    """Очищает базу банных"""
    await session_db.execute(delete(Processing))
    await session_db.execute(delete(Resumes))
    await session_db.execute(delete(Requirements))
    await session_db.execute(delete(Users))
    await session_db.commit()


@pytest_asyncio.fixture(scope="function", autouse=True)
async def clearing_redis():
    redis = get_redis()
    await redis.flushall()
    await redis.aclose()
    return redis