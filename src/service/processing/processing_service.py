from typing import List

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import ProcessingStatus, Processing
from src.exeptions.service_exc import InsertionErrorService, NoRightsService, ResourceNotFound
from src.repository.database.processing import ProcessingRepository
from src.repository.redis.processing_cache import ProcessingCacheRepository
from src.schemas.response import ProcessingOut
from src.service.config.schemas import Config


class ProcessingService:

    def __init__(
        self,
        processing_repo: ProcessingRepository,
        processing_cache_repo: ProcessingCacheRepository,
        session_db: AsyncSession,
        config: Config,
    ):
        self.processing_repo = processing_repo
        self.processing_cache_repo = processing_cache_repo
        self.session_db = session_db
        self.conf = config

    def _check_rights(self, expected_user_id: int, current_user_id: int):
        """
        :raise NoRightsService: Если ожидаемый ID пользователя не совпал с текущим
        """
        if expected_user_id != current_user_id:
            raise NoRightsService()

    async def create_processing(
        self,
        processing_id: int,
        resume_id: int,
        requirement_id: int,
        user_id: int
    ) -> Processing:
        """
        Всегда сперва создаётся со статусом status == `in_progress`
        :except InsertionErrorService: Из-за отсутствия указанного ID или из-за уже существовании указанного processing_id
        """
        try:
            tx_ctx = self.session_db.begin_nested() if self.session_db.in_transaction() else self.session_db.begin()

            async with tx_ctx:
                processing = await self.processing_repo.add_processing(
                    processing_id=processing_id,
                    resume_id=resume_id,
                    requirement_id=requirement_id,
                    user_id=user_id,
                    status=ProcessingStatus.IN_PROGRESS,
                    success=False
                )

                # flush чтобы поймать IntegrityError здесь
                await self.session_db.flush()

            await self.session_db.commit()

        except IntegrityError as e:
            raise InsertionErrorService() from e

        await self.processing_cache_repo.set_by_resume(processing)
        return processing

    async def get_processing_by_resume(self, resume_id: int, user_id: int) -> Processing:
        """
        :raise NoRightsService: При недостатке прав у пользователя на просмотр данных
        :raise ResourceNotFound: Если данные не найдены
        """
        processing_redis = await self.processing_cache_repo.get_by_resume(resume_id=resume_id)

        if processing_redis:
            self._check_rights(processing_redis.user_id, user_id)
            return processing_redis

        processing_db = await self.processing_repo.get_by_resume(resume_id=resume_id)

        if processing_db:
            self._check_rights(processing_db.user_id, user_id)
            await self.processing_cache_repo.set_by_resume(processing_db)
            return processing_db

        raise ResourceNotFound()

    async def delete_processing(self, processing_ids: List[int], resume_ids: List[int]):
        await self.processing_repo.delete_processing(processing_ids)
        await self.session_db.commit()
        await self.processing_cache_repo.delete_by_resume(resume_ids)

    async def update_processing(
        self,
        processing_id: int,
        status: ProcessingStatus | None = None,
        success: bool | None = None,
        message_error: str | None = None,
        wait_seconds: int | None = None,
        score: int | None = None,
        matches: str | None = None,
        recommendation: str | None = None,
        verdict: str | None = None,
    ):
        updated_processing = await self.processing_repo.update_processing(
            processing_id=processing_id,
            status=status,
            success=success,
            message_error=message_error,
            wait_seconds=wait_seconds,
            score=score,
            matches=matches,
            recommendation=recommendation,
            verdict=verdict
        )

        # если что-то вернулось, значит обновили БД новыми данными
        if updated_processing:
            await self.session_db.commit()
            await self.processing_cache_repo.set_by_resume(updated_processing)
