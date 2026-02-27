from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.core import get_db
from src.repository.database import RequirementRepository, get_requirement_repository
from src.repository.redis import RequirementCacheRepository, get_requirement_cache_repository
from src.service.config import get_config
from src.service.config.schemas import Config
from src.service.requirements.requirements_service import RequirementService
from src.service.resumes import get_resume_service, ResumeService


async def get_requirement_service(
    resume_service: ResumeService = Depends(get_resume_service),
    requirement_repo: RequirementRepository = Depends(get_requirement_repository),
    requirement_cache_repo: RequirementCacheRepository = Depends(get_requirement_cache_repository),
    db: AsyncSession = Depends(get_db),
    conf: Config = Depends(get_config),
) -> RequirementService:
    return RequirementService(
        resume_service=resume_service,
        requirement_repo=requirement_repo,
        requirement_cache_repo=requirement_cache_repo,
        session_db=db,
        config=conf
    )