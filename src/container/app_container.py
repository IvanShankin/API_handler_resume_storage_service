from contextlib import asynccontextmanager
from typing import Optional

from src.database.core import get_db
from src.infrastructure.redis import get_redis
from src.repository.database import UserRepository, RequirementRepository, ResumeRepository, ProcessingRepository
from src.repository.redis import (
    UserCacheRepository,
    RequirementCacheRepository,
    ResumeCacheRepository,
    ProcessingCacheRepository,
)
from src.repository.redis.kafka_message_cache import KafkaMessageCacheRepository
from src.service.config import get_config
from src.service.kafka.kafka_event_handler import KafkaEventHandlerService
from src.service.processing.processing_service import ProcessingService
from src.service.requirements.requirements_service import RequirementService
from src.service.resumes.resumes_service import ResumeService
from src.service.users import UserService
from src.service.utils.logger import get_logger


@asynccontextmanager
async def resolve_dep(dep):
    gen = dep()
    value = await gen.__anext__()

    try:
        yield value
    finally:
        await gen.aclose()


class Container:

    def __init__(self):

        self.config = get_config()
        self.logger = get_logger()
        self.session_redis = get_redis()

    async def get_user_service(self) -> UserService:

        async with resolve_dep(get_db) as session:
            user_repo = UserRepository(
                session,
                self.config
            )
            cache_repo = UserCacheRepository(
                self.session_redis,
                self.config
            )


            return UserService(
                user_repo=user_repo,
                cache_repo=cache_repo,
                session_db=session,
                config=self.config,
            )

    async def get_event_handler_service(self) -> KafkaEventHandlerService:
        kafka_cache_repo = KafkaMessageCacheRepository(
            config=self.config,
            redis_session=self.session_redis
        )

        return  KafkaEventHandlerService(
            user_service=await self.get_user_service(),
            requirement_service=await self.get_requirement_service(),
            resume_service=await self.get_resume_service(),
            processing_service=await self.get_processing_service(),
            kafka_message_cache=kafka_cache_repo,
            config=self.config,
            logger=self.logger,
        )

    async def get_processing_service(self) -> ProcessingService:

        async with resolve_dep(get_db) as session:
            processing_repo = ProcessingRepository(
                session,
                self.config
            )
            processing_cache_repo = ProcessingCacheRepository(
                self.session_redis,
                self.config
            )

            return ProcessingService(
                processing_repo=processing_repo,
                processing_cache_repo=processing_cache_repo,
                session_db=session,
                config=self.config,
            )

    async def get_resume_service(self) -> ResumeService:

        async with resolve_dep(get_db) as session:
            resume_repo = ResumeRepository(
                session,
                self.config
            )
            resume_cache_repo = ResumeCacheRepository(
                self.session_redis,
                self.config
            )

            return ResumeService(
                processing_service=await self.get_processing_service(),
                resume_repo=resume_repo,
                resume_cache_repo=resume_cache_repo,
                session_db=session,
                config=self.config,
            )

    async def get_requirement_service(self) -> RequirementService:

        async with resolve_dep(get_db) as session:
            requirement_repo = RequirementRepository(
                session,
                self.config
            )
            requirement_cache_repo = RequirementCacheRepository(
                self.session_redis,
                self.config
            )

            return RequirementService(
                resume_service=await self.get_resume_service(),
                requirement_repo=requirement_repo,
                requirement_cache_repo=requirement_cache_repo,
                session_db=session,
                config=self.config,
            )



_container: Optional[Container] = None


def init_container() -> Container:
    global _container
    if _container is None:
        _container = Container()

    return _container


def set_container(container: Container) -> None:
    global _container
    _container = container


def get_container() -> Container:
    global _container
    if _container is None:
        raise RuntimeError("Service not initialized")
    return _container
