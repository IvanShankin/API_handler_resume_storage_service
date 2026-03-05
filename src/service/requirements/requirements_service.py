from sqlite3 import IntegrityError
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import Requirements
from src.exeptions.service_exc import IDAlreadyExists, NoRightsService, ResourceNotFound
from src.repository.database.requirement import RequirementRepository
from src.repository.redis.requirement_cache import RequirementCacheRepository
from src.service.config.schemas import Config
from src.service.resumes.resumes_service import ResumeService


class RequirementService:

    def __init__(
        self,
        resume_service: ResumeService,
        requirement_repo: RequirementRepository,
        requirement_cache_repo: RequirementCacheRepository,
        session_db: AsyncSession,
        config: Config,
    ):
        self.resume_service = resume_service
        self.requirement_repo = requirement_repo
        self.requirement_cache_repo = requirement_cache_repo

        self.session_db = session_db
        self.conf = config

    def _check_rights(self, expected_user_id: int, current_user_id: int):
        """
        :raise NoRightsService: Если ожидаемый ID пользователя не совпал с текущим
        """
        if expected_user_id != current_user_id:
            raise NoRightsService()

    async def create_requirement(
        self,
        requirement_id: int,
        user_id: int,
        requirement: str
    ) -> Requirements:
        """
        :except IDAlreadyExists: Из-за существования указанного ID
        """
        try:
            tx_ctx = self.session_db.begin_nested() if self.session_db.in_transaction() else self.session_db.begin()

            async with tx_ctx:
                requirement_db = await self.requirement_repo.add_requirement(
                    requirement_id=requirement_id,
                    user_id=user_id,
                    requirements=requirement,
                )

                # flush чтобы поймать IntegrityError здесь
                await self.session_db.flush()

        except IntegrityError as e:
            raise IDAlreadyExists() from e

        await self.requirement_cache_repo.set_by_user(
            user_id=user_id,
            requirements=await self.requirement_repo.get_by_user(user_id)
        )
        return requirement_db

    async def get_requirements_by_user(self, user_id: int) -> List[Requirements]:
        """
        :raise NoRightsService: При недостатке прав у пользователя на просмотр данных
        """
        requirements_redis = await self.requirement_cache_repo.get_by_user(user_id=user_id)
        if not requirements_redis:
            requirements_db = await self.requirement_repo.get_by_user(user_id=user_id)
            if requirements_db:
                self._check_rights(requirements_db[0].user_id, user_id)
                await self.requirement_cache_repo.set_by_user(
                    user_id=user_id,
                    requirements=requirements_db
                )

            return requirements_db

        if requirements_redis:
            self._check_rights(requirements_redis[0].user_id, user_id)

        return requirements_redis

    async def get_requirement(self, requirement_id: int, user_id: int) -> Requirements | None:
        """
        :raise NoRightsService: При недостатке прав у пользователя на просмотр данных
        :raise ResourceNotFound: Если данные не найдены
        """
        requirement = await self.requirement_repo.get(requirement_id=requirement_id)

        if requirement:
            self._check_rights(requirement.user_id, user_id)
            return requirement

        raise ResourceNotFound()

    async def delete_requirement(
        self,
        requirement_ids: List[int],
        resume_ids: List[int],
        processing_ids: List[int],
        user_id: int
    ) -> None:
        await self.resume_service.delete_resume(
            resume_ids=resume_ids,
            requirement_ids=requirement_ids,
            processing_ids=processing_ids
        )
        await self.requirement_repo.delete_requirements(requirement_ids=requirement_ids)

        await self.requirement_cache_repo.set_by_user(
            user_id=user_id,
            requirements=await self.requirement_repo.get_by_user(user_id=user_id)
        )