from typing import List

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import Resumes
from src.service.config.schemas import Config


class ResumeRepository:

    def __init__(self, session_db: AsyncSession, config: Config):
        self.session_db = session_db
        self.conf = config

    async def add_resume(
        self,
        resume_id: int,
        user_id: int,
        requirement_id: int,
        resume: str
    ) -> Resumes:
        new_resume = Resumes(
            resume_id=resume_id,
            requirement_id=requirement_id,
            user_id=user_id,
            resume=resume,
        )

        self.session_db.add(new_resume)

        return new_resume

    async def get_by_requirement(self, requirement_id: int) -> List[Resumes]:
        result_db = await self.session_db.execute(
            select(Resumes)
            .where(Resumes.requirement_id == requirement_id)
        )
        return result_db.scalars().all()

    async def get(self, resume_id: int) -> Resumes | None:
        result_db = await self.session_db.execute(
            select(Resumes)
            .where(Resumes.resume_id == resume_id)
        )
        return result_db.scalar_one_or_none()

    async def delete_resume(self, resume_ids: List[int]):
        await self.session_db.execute(
            delete(Resumes)
            .where(Resumes.resume_id.in_(resume_ids))
        )