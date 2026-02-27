from datetime import datetime
from typing import Optional, List

from sqlalchemy import update, delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import ProcessingStatus, Processing
from src.service.config.schemas import Config


class ProcessingRepository:

    def __init__(self, session_db: AsyncSession, config: Config):
        self.session_db = session_db
        self.conf = config

    async def add_processing(
        self,
        processing_id: int,
        resume_id: int,
        requirement_id: int,
        user_id: int,
        status: ProcessingStatus,
        success: bool,

        # только при success == False
        message_error: Optional[str] = None,
        wait_seconds: Optional[int] = None,

        # только при success == True
        score: Optional[int] = None,
        matches: Optional[str] = None,
        recommendation: Optional[str] = None,
        verdict: Optional[str] = None,

        created_at: Optional[datetime] = None
    ) -> Processing:
        new_processing = Processing(
            processing_id=processing_id,
            resume_id=resume_id,
            requirement_id=requirement_id,
            user_id=user_id,
            status=status,
            success=success,
            message_error=message_error,
            wait_seconds=wait_seconds,
            score=score,
            matches=matches,
            recommendation=recommendation,
            verdict=verdict,
            create_at=created_at
        )

        self.session_db.add(new_processing)

        return new_processing

    async def get_by_resume(self, resume_id: int) -> Processing | None:
        result_db = await self.session_db.execute(
            select(Processing)
            .where(Processing.resume_id == resume_id)
        )
        return result_db.scalar_one_or_none()

    async def update_processing(
        self,
        processing_id: int,
        status: ProcessingStatus | None,
        success: bool | None,

        # только при success == False
        message_error: str | None,
        wait_seconds: int | None,

        # только при success == True
        score: int | None,
        matches: str | None,
        recommendation: str | None,
        verdict: str | None,
    ) -> Processing | None:
        data_for_update = {}

        if processing_id:
            data_for_update["processing_id"] = processing_id
        if status:
            data_for_update["status"] = status
        if success:
            data_for_update["success"] = success
        if message_error:
            data_for_update["message_error"] = message_error
        if wait_seconds:
            data_for_update["wait_seconds"] = wait_seconds
        if score:
            data_for_update["score"] = score
        if matches:
            data_for_update["matches"] = matches
        if recommendation:
            data_for_update["recommendation"] = recommendation
        if verdict:
            data_for_update["verdict"] = verdict

        if data_for_update:
            result_db = await self.session_db.execute(
                update(Processing)
                .where(Processing.processing_id == processing_id)
                .values(
                    **data_for_update
                )
                .returning(Processing)
            )
            return result_db.scalar_one_or_none()

        return None

    async def delete_processing(self, processing_ids: List[int]):
        await self.session_db.execute(
            delete(Processing)
            .where(Processing.processing_id.in_(processing_ids))
        )
