from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.core import get_db
from src.repository.database import ResumeRepository, get_resume_repository
from src.repository.redis import ResumeCacheRepository, get_resume_cache_repository
from src.service.config import get_config
from src.service.config.schemas import Config
from src.service.processing import ProcessingService, get_processing_service
from src.service.resumes.resumes_service import ResumeService


async def get_resume_service(
    db: AsyncSession = Depends(get_db),
    conf: Config = Depends(get_config),
    processing_service: ProcessingService = Depends(get_processing_service),
    resume_repo: ResumeRepository = Depends(get_resume_repository),
    resume_cache_repo: ResumeCacheRepository = Depends(get_resume_cache_repository),
) -> ResumeService:
    return ResumeService(
        processing_service=processing_service,
        resume_repo=resume_repo,
        resume_cache_repo=resume_cache_repo,
        session_db=db,
        config=conf
    )