from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.core import get_db
from src.repository.database import get_processing_repository, ProcessingRepository
from src.repository.redis import get_processing_cache_repository, ProcessingCacheRepository
from src.service.config import get_config
from src.service.config.schemas import Config
from src.service.processing.processing_service import ProcessingService


async def get_processing_service(
    db: AsyncSession = Depends(get_db),
    conf: Config = Depends(get_config),
    processing_repo: ProcessingRepository = Depends(get_processing_repository),
    processing_cache_repo: ProcessingCacheRepository = Depends(get_processing_cache_repository),
) -> ProcessingService:
    return ProcessingService(
        processing_repo=processing_repo,
        processing_cache_repo=processing_cache_repo,
        session_db=db,
        config=conf
    )