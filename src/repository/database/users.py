from datetime import datetime

from pydantic import EmailStr
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import Users
from src.service.config.schemas import Config


class UserRepository:

    def __init__(self, session_db: AsyncSession, config: Config):
        self.session_db = session_db
        self.conf = config

    async def get_user(self, user_id: int) -> Users | None:
        user = await self.session_db.execute(select(Users).where(Users.user_id == user_id))
        return user.scalar_one_or_none()

    async def add_user(
        self,
        user_id: int,
        username: str | EmailStr,
        full_name: str,
        created_at: datetime,
    ) -> Users:

        new_user = Users(
            user_id=user_id,
            username=username,
            full_name=full_name,
            created_at=created_at
        )

        self.session_db.add(new_user)

        return new_user