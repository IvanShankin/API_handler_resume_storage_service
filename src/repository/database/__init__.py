from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.core import get_db
from src.repository.database.processing import ProcessingRepository
from src.repository.database.requirement import RequirementRepository
from src.repository.database.resume import ResumeRepository
from src.repository.database.users import UserRepository
from src.service.config import get_config
from src.service.config.schemas import Config


async def get_processing_repository(
    db: AsyncSession = Depends(get_db),
    conf: Config = Depends(get_config)
) -> ProcessingRepository:
    return ProcessingRepository(db, conf)


async def get_requirement_repository(
    db: AsyncSession = Depends(get_db),
    conf: Config = Depends(get_config)
) -> RequirementRepository:
    return RequirementRepository(db, conf)


async def get_resume_repository(
    db: AsyncSession = Depends(get_db),
    conf: Config = Depends(get_config)
) -> ResumeRepository:
    return ResumeRepository(db, conf)


async def get_user_repository(
    db: AsyncSession = Depends(get_db),
    conf: Config = Depends(get_config)
) -> UserRepository:
    return UserRepository(db, conf)