import asyncio
import threading

import uvicorn
import os
from dotenv import load_dotenv
from fastapi import FastAPI

from src.api.requests import main_router
from src.database.creating import create_database
from src.dependencies.kafka_dependencies import consumer
from src.service.config import init_config

load_dotenv()
KAFKA_TOPIC_CONSUMER_FOR_UPLOADING_DATA = os.getenv('KAFKA_TOPIC_CONSUMER_FOR_UPLOADING_DATA')

app = FastAPI(
    title="Storage Service"
)

app.include_router(main_router)

async def on_startup():
    conf = init_config()

    await create_database()

    def run_consumer():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(consumer.consumer_run())

    consumer_thread = threading.Thread(target=run_consumer)
    consumer_thread.daemon = True  # Демонизируем поток, чтобы он завершился при завершении основного потока
    consumer_thread.start()


async def on_shutdown():
    pass


if __name__ == '__main__':
    asyncio.run(on_startup())
    try:
        uvicorn.run(
            "main:app",
            host="0.0.0.0",
            port=8000,
            reload=True
        )
    finally:
        asyncio.run(on_shutdown())