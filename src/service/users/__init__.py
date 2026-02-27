from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.core import get_db
from src.repository.database import UserRepository, get_user_repository
from src.repository.redis import UserCacheRepository, get_user_cache_repository
from src.service.config import get_config
from src.service.config.schemas import Config
from src.service.users.users_service import UsersService


__all__ = [
    "UsersService"
]


async def get_users_service(
    user_repo: UserRepository = Depends(get_user_repository),
    cache_repo: UserCacheRepository = Depends(get_user_cache_repository),
    session_db: AsyncSession = Depends(get_db),
    conf: Config = Depends(get_config),
) -> UsersService:
    return UsersService(
        user_repo=user_repo,
        cache_repo=cache_repo,
        session_db=session_db,
        config=conf,
    )