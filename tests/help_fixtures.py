from typing import AsyncGenerator

import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from src.api.app import get_app
from src.database.core import get_db
from src.service.config import get_config


@pytest_asyncio.fixture
async def session_db() -> AsyncGenerator[AsyncSession, None]:
    engine = create_async_engine(get_config().db_connection.sql_db_url)
    session_local = sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False
    )
    db = session_local()
    try:
        yield db
    finally:
        await db.close()


@pytest_asyncio.fixture
async def client_with_db(session_db):  # session_db открываем заранее
    app = get_app()

    # переопределяем Depends(get_db) на уже открытую сессию
    app.dependency_overrides[get_db] = lambda: session_db
    async with AsyncClient(transport=ASGITransport(get_app()), base_url="http://test") as ac:
        yield ac


