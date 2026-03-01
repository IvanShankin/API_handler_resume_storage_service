import random
import string

import pytest_asyncio
from datetime import datetime, UTC
from typing import Tuple, Optional

from sqlalchemy import select, func

from helper_func import create_accesses_token
from src.database.models import Users, Resumes, Processing, Requirements
from src.infrastructure.redis.core import get_redis
from src.repository.database import UserRepository, ResumeRepository, ProcessingRepository, RequirementRepository
from src.repository.redis import UserCacheRepository, ResumeCacheRepository, ProcessingCacheRepository, \
    RequirementCacheRepository, KafkaMessageCacheRepository
from src.service.config import get_config
from src.service.kafka import KafkaEventHandlerService
from src.service.processing import ProcessingService
from src.service.requirements import RequirementService
from src.service.resumes import ResumeService
from src.service.users import UserService
from src.service.utils.logger import get_logger


@pytest_asyncio.fixture
async def user_service_fix(session_db) -> UserService:
    conf = get_config()
    return UserService(
        user_repo=UserRepository(
            session_db=session_db,
            config=conf,
        ),
        cache_repo=UserCacheRepository(
            redis_session=get_redis(),
            config=conf,
        ),
        session_db=session_db,
        config=conf,
    )


@pytest_asyncio.fixture
async def create_user(user_service_fix, session_db):
    async def _factory() -> Tuple[Users, str]:
        result = await session_db.execute(
            select(func.max(Users.user_id))
        )
        max_id = result.scalar_one_or_none()
        next_id = (max_id or 0) + 1

        new_user = await user_service_fix.create_user(
            user_id=next_id,
            username=f"{random.choice(string.ascii_letters)}@mail.com",
            full_name="test_username",
            created_at=datetime.now(UTC)
        )
        accesses_token = create_accesses_token(new_user.user_id)

        return new_user, accesses_token

    yield _factory


@pytest_asyncio.fixture
async def requirement_service_fix(session_db, resume_service_fix) -> RequirementService:
    conf = get_config()
    return RequirementService(
        resume_service=resume_service_fix,
        requirement_repo=RequirementRepository(
            session_db=session_db,
            config=conf,
        ),
        requirement_cache_repo=RequirementCacheRepository(
            redis_session=get_redis(),
            config=conf,
        ),
        session_db=session_db,
        config=conf,
    )


@pytest_asyncio.fixture
async def create_requirement(requirement_service_fix, create_user, session_db):
    async def _factory(user_id: Optional[int] = None) -> Requirements:
        result = await session_db.execute(
            select(func.max(Requirements.requirement_id))
        )
        max_id = result.scalar_one_or_none()
        next_id = (max_id or 0) + 1

        if not user_id:
            new_user, accesses_token = await create_user()
            user_id = new_user.user_id

        new_requirement = await requirement_service_fix.create_requirement(
            requirement_id=next_id,
            user_id=user_id,
            requirements="Тестовое требование",
        )

        return new_requirement

    yield _factory


@pytest_asyncio.fixture
async def resume_service_fix(session_db, processing_service_fix) -> ResumeService:
    conf = get_config()
    return ResumeService(
        processing_service=processing_service_fix,
        resume_repo=ResumeRepository(
            session_db=session_db,
            config=conf,
        ),
        resume_cache_repo=ResumeCacheRepository(
            redis_session=get_redis(),
            config=conf,
        ),
        session_db=session_db,
        config=conf,
    )


@pytest_asyncio.fixture
async def create_resume(resume_service_fix, create_user, create_requirement, session_db):
    async def _factory(user_id: Optional[int] = None, requirement_id: Optional[int] = None) -> Resumes:
        result = await session_db.execute(
            select(func.max(Resumes.resume_id))
        )
        max_id = result.scalar_one_or_none()
        next_id = (max_id or 0) + 1

        if user_id is None and requirement_id is None:
            new_user, accesses_token = await create_user()
            user_id = new_user.user_id

        if requirement_id is None:
            requirement = await create_requirement(user_id)
            requirement_id = requirement.requirement_id
            user_id = requirement.user_id

        new_resume = await resume_service_fix.create_resume(
            resume_id=next_id,
            requirement_id=requirement_id,
            user_id=user_id,
            resume="Тестовое резюме",
        )

        return new_resume

    yield _factory


@pytest_asyncio.fixture
async def processing_service_fix(session_db) -> ProcessingService:
    conf = get_config()
    return ProcessingService(
        processing_repo=ProcessingRepository(
            session_db=session_db,
            config=conf,
        ),
        processing_cache_repo=ProcessingCacheRepository(
            redis_session=get_redis(),
            config=conf,
        ),
        session_db=session_db,
        config=conf,
    )


@pytest_asyncio.fixture
async def create_processing(processing_service_fix, create_resume, session_db):
    async def _factory(user_id: Optional[int] = None) -> Processing:
        result = await session_db.execute(
            select(func.max(Processing.processing_id))
        )
        max_id = result.scalar_one_or_none()
        next_id = (max_id or 0) + 1

        resume = await create_resume(user_id)

        new_processing = await processing_service_fix.create_processing(
            processing_id=next_id,
            resume_id=resume.resume_id,
            requirement_id=resume.requirement_id,
            user_id=resume.user_id,
        )

        return new_processing

    yield _factory


@pytest_asyncio.fixture
async def kafka_event_handler_fix(
    user_service_fix,
    requirement_service_fix,
    resume_service_fix,
    processing_service_fix,
) -> KafkaEventHandlerService:
    conf = get_config()
    return KafkaEventHandlerService(
        user_service=user_service_fix,
        requirement_service=requirement_service_fix,
        resume_service=resume_service_fix,
        processing_service=processing_service_fix,
        kafka_message_cache= KafkaMessageCacheRepository(
            redis_session=get_redis(),
            config=conf,
        ),
        logger=get_logger(__name__),
        config=conf,
    )