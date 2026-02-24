from sqlalchemy.ext.asyncio import AsyncSession
from contextlib import asynccontextmanager

from src.service.config.config_core import get_config


@asynccontextmanager
async def get_db() -> AsyncSession:
    async_session_factory = get_config().db_connection.session_local
    async with async_session_factory() as session:
        yield session