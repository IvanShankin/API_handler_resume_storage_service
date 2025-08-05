import asyncio
import uvicorn
import os
from dotenv import load_dotenv
from fastapi import FastAPI

from srt.database.database import create_database
from srt.requests import main_router
from srt.dependencies import check_exists_topic

load_dotenv()
KAFKA_TOPIC_PRODUCER_FOR_UPLOADING_DATA = os.getenv('KAFKA_TOPIC_PRODUCER_FOR_UPLOADING_DATA')

app = FastAPI()

app.include_router(main_router)

if __name__ == '__main__':
    check_exists_topic([KAFKA_TOPIC_PRODUCER_FOR_UPLOADING_DATA])
    asyncio.run(create_database())
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8005,
        reload=True
    )