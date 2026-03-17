from typing import List, Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import Resumes
from src.exeptions.service_exc import InsertionErrorService, NoRightsService, ResourceNotFound
from src.repository.database.resume import ResumeRepository
from src.repository.redis.resume_cache import ResumeCacheRepository
from src.schemas.request import ResumeSortField
from src.schemas.response import ResumeOut
from src.service.config.schemas import Config
from src.service.processing.processing_service import ProcessingService


class ResumeService:

    def __init__(
        self,
        processing_service: ProcessingService,
        resume_repo: ResumeRepository,
        resume_cache_repo: ResumeCacheRepository,
        session_db: AsyncSession,
        config: Config,
    ):
        self.processing_service = processing_service
        self.resume_repo = resume_repo
        self.resume_cache_repo = resume_cache_repo
        self.session_db = session_db
        self.conf = config

    def _check_rights(self, expected_user_id: int, current_user_id: int):
        """
        :raise NoRightsService: Если ожидаемый ID пользователя не совпал с текущим
        """
        if expected_user_id != current_user_id:
            raise NoRightsService()

    def sort_resumes(
        self,
        resumes: List[Resumes],
        sort_resume: Optional[ResumeSortField] = None
    ) -> List[Resumes]:

        if not sort_resume:
            return resumes

        if sort_resume.created_desc:
            return sorted(resumes, key=lambda r: r.created_at, reverse=True)

        if sort_resume.created_asc:
            return sorted(resumes, key=lambda r: r.created_at)

        return resumes

    async def create_resume(
        self,
        resume_id: int,
        requirement_id: int,
        user_id: int,
        resume: str
    ) -> Resumes:
        """
        :except InsertionErrorService: Из-за отсутствия указанного ID или из-за уже существовании указанного resume_id
        """
        try:
            tx_ctx = self.session_db.begin_nested() if self.session_db.in_transaction() else self.session_db.begin()

            async with tx_ctx:
                resume = await self.resume_repo.add_resume(
                    resume_id=resume_id,
                    requirement_id=requirement_id,
                    user_id=user_id,
                    resume=resume,
                )

                # flush чтобы поймать IntegrityError здесь
                await self.session_db.flush()

            await self.session_db.commit()

        except IntegrityError as e:
            raise InsertionErrorService() from e

        await self.resume_cache_repo.set_by_requirement(
            requirement_id=requirement_id,
            resumes=await self.resume_repo.get_by_requirement(requirement_id)
        )
        return resume

    async def get_resume_by_requirements(
        self,
        requirement_id: int,
        user_id: int,
        sort: Optional[ResumeSortField] = None
    ) -> List[Resumes]:
        """
        :raise NoRightsService: При недостатке прав у пользователя на просмотр данных
        """
        resumes_redis = await self.resume_cache_repo.get_by_requirement(requirement_id)
        if not resumes_redis:
            resumes_db = await self.resume_repo.get_by_requirement(requirement_id=requirement_id)
            if resumes_db:
                self._check_rights(resumes_db[0].user_id, user_id)

                await self.resume_cache_repo.set_by_requirement(
                    requirement_id=requirement_id,
                    resumes=resumes_db
                )

            return self.sort_resumes(resumes_db, sort)

        if resumes_redis:
            self._check_rights(resumes_redis[0].user_id, user_id)

        return self.sort_resumes(resumes_redis, sort)

    async def get_resume(self, resume_id: int, user_id: int) -> Resumes:
        """
        :raise NoRightsService: При недостатке прав у пользователя на просмотр данных
        :raise ResourceNotFound: Если данные не найдены
        """
        resume = await self.resume_repo.get(resume_id=resume_id)

        if resume:
            self._check_rights(resume.user_id, user_id)
            return resume

        raise ResourceNotFound()

    async def delete_resume(self, resume_ids: List[int], requirement_ids: List[int], processing_ids: List[int]) -> None:
        await self.processing_service.delete_processing(processing_ids=processing_ids, resume_ids=resume_ids)
        await self.resume_repo.delete_resume(resume_ids=resume_ids)
        await self.session_db.commit()

        for requirement_id in requirement_ids:
            await self.resume_cache_repo.set_by_requirement(
                requirement_id=requirement_id,
                resumes=await self.resume_repo.get_by_requirement(requirement_id=requirement_id)
            )
