from typing import List

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import Resumes, Requirements
from src.service.config.schemas import Config


class RequirementRepository:

    def __init__(self, session_db: AsyncSession, config: Config):
        self.session_db = session_db
        self.conf = config

    async def add_requirement(
        self,
        requirement_id: int,
        user_id: int,
        requirement: str,
    ) -> Requirements:
        new_requirement = Requirements(
            requirement_id=requirement_id,
            user_id=user_id,
            requirement=requirement,
        )

        self.session_db.add(new_requirement)

        return new_requirement

    async def get_by_user(self, user_id: int) -> List[Requirements]:
        result_db = await self.session_db.execute(
            select(Requirements)
            .where(Requirements.user_id == user_id)
        )
        return result_db.scalars().all()

    async def get(self, requirement_id: int) -> Requirements | None:
        result_db = await self.session_db.execute(
            select(Requirements)
            .where(Requirements.requirement_id == requirement_id)
        )
        return result_db.scalar_one_or_none()

    async def delete_requirements(self, requirement_ids: List[int]):
        await self.session_db.execute(
            delete(Requirements)
            .where(Requirements.requirement_id.in_(requirement_ids))
        )