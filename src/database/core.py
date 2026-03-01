from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession

from src.service.config.config_core import get_config


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async_session_factory = get_config().db_connection.session_local
    async with async_session_factory() as session:
        yield session