from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI

from src.api.exception_handler import register_exception_handlers
from src.api.requests import main_router
from src.container.app_container import init_container
from src.database.creating import create_database
from src.infrastructure.kafka.admin_client import init_admin_client, shutdown_admin_client
from src.infrastructure.kafka.consumers.run_consumers import run_consumer
from src.infrastructure.kafka.topic_manager import check_exists_topic
from src.infrastructure.redis import init_redis, close_redis
from src.service.config import init_config

_app: Optional[FastAPI] = None


def _include_router(app: FastAPI):
    app.include_router(main_router)


def init_fastapi_app() -> FastAPI:
    global _app
    app = FastAPI(
        title="Storage Service",
        lifespan=lifespan
    )
    _include_router(app)
    register_exception_handlers(app)
    _app = app

    return app


def get_app():
    global _app
    if _app is None:
        raise RuntimeError("FastAPI App not initialized")
    return _app


@asynccontextmanager
async def lifespan(app: FastAPI):
    conf = init_config()
    await init_redis()
    await init_admin_client()
    init_container()

    await create_database()

    await check_exists_topic(conf.kafka_topics.all_topics)
    consumer_runner = await run_consumer()

    try:
        yield
    finally:
        await consumer_runner.stop()
        await close_redis()
        await shutdown_admin_client()
