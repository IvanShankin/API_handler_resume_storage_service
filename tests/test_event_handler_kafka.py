from datetime import datetime, UTC

import pytest
from sqlalchemy import select, func

from helper_func import FakeKafkaMessage
from src.database.models import Resumes, Requirements, ProcessingStatus, Processing
from src.exeptions.service_exc import ResourceNotFound
from src.schemas.kafka_data import NewUser, NewResume, NewRequirement, DeleteResume, DeleteRequirements, \
    DeleteProcessing, EndProcessingReceived, NewProcessing


class TestEventHandlerKafka:

    @pytest.mark.asyncio
    async def test_new_user(self, kafka_event_handler_fix, user_service_fix):
        msg = FakeKafkaMessage(
            data=NewUser(
                user_id=1,
                username="test_name@mail.com",
                full_name="full_name",
                created_at=datetime.now(UTC)
            ).model_dump(),
            key=kafka_event_handler_fix.conf.consumer_keys.new_user,
        )

        await kafka_event_handler_fix.handler_messages(msg)

        assert await user_service_fix.get_user(1)

    @pytest.mark.asyncio
    async def test_new_resume(self, kafka_event_handler_fix, session_db, create_user, create_requirement,resume_service_fix):
        result = await session_db.execute(
            select(func.max(Resumes.resume_id))
        )
        max_id = result.scalar_one_or_none()
        next_id = (max_id or 0) + 1

        new_user, _ = await create_user()
        new_requirement = await create_requirement(new_user.user_id)

        payload = NewResume(
            resume_id=next_id,
            user_id=new_user.user_id,
            requirement_id=new_requirement.requirement_id,
            resume="Тестовое резюме из kafka"
        ).model_dump()

        msg = FakeKafkaMessage(
            data=payload,
            key=kafka_event_handler_fix.conf.consumer_keys.new_resume,
        )

        await kafka_event_handler_fix.handler_messages(msg)

        assert await resume_service_fix.get_resume(resume_id=next_id, user_id=new_user.user_id)

    @pytest.mark.asyncio
    async def test_new_requirement(self, kafka_event_handler_fix, session_db, create_user, requirement_service_fix):
        result = await session_db.execute(
            select(func.max(Requirements.requirement_id))
        )
        max_id = result.scalar_one_or_none()
        next_id = (max_id or 0) + 1

        new_user, _ = await create_user()

        payload = NewRequirement(
            requirement_id=next_id,
            user_id=new_user.user_id,
            requirement="Тестовое требование из kafka"
        ).model_dump()

        msg = FakeKafkaMessage(
            data=payload,
            key=kafka_event_handler_fix.conf.consumer_keys.new_requirements,
        )

        await kafka_event_handler_fix.handler_messages(msg)

        assert await requirement_service_fix.get_requirement(requirement_id=next_id, user_id=new_user.user_id)

    @pytest.mark.asyncio
    async def test_new_processing(self, kafka_event_handler_fix, session_db, create_resume, processing_service_fix):
        result = await session_db.execute(
            select(func.max(Processing.processing_id))
        )
        max_id = result.scalar_one_or_none()
        next_id = (max_id or 0) + 1

        resume = await create_resume()

        payload = NewProcessing(
            processing_id=next_id,
            resume_id=resume.resume_id,
            requirement_id=resume.requirement_id,
            user_id=resume.user_id,
        ).model_dump()

        msg = FakeKafkaMessage(
            data=payload,
            key=kafka_event_handler_fix.conf.consumer_keys.new_processing,
        )

        await kafka_event_handler_fix.handler_messages(msg)

        assert await processing_service_fix.get_processing_by_resume(resume_id=resume.resume_id, user_id=resume.user_id)

    @pytest.mark.asyncio
    async def test_end_processing_success_and_failed(self, kafka_event_handler_fix, create_processing, processing_service_fix):
        processing = await create_processing()
        pid = processing.processing_id

        # success = True -> должен записаться статус SUCCESSFULLY
        payload_success = EndProcessingReceived(
            processing_id=pid,
            success=True,
        ).model_dump()
        msg_success = FakeKafkaMessage(
            data=payload_success,
            key=kafka_event_handler_fix.conf.consumer_keys.end_processing,
        )
        await kafka_event_handler_fix.handler_messages(msg_success)

        p = await processing_service_fix.get_processing_by_resume(
            resume_id=processing.resume_id,
            user_id=processing.user_id
        )
        assert p is not None
        assert p.status == ProcessingStatus.SUCCESSFULLY

        # success = False -> должен записаться статус FAILED
        payload_failed = EndProcessingReceived(
            processing_id=pid,
            success=False,
        ).model_dump()
        msg_failed = FakeKafkaMessage(
            data=payload_failed,
            key=kafka_event_handler_fix.conf.consumer_keys.end_processing,
            offset=2
        )
        await kafka_event_handler_fix.handler_messages(msg_failed)

        p2 = await processing_service_fix.get_processing_by_resume(
            resume_id=processing.resume_id,
            user_id=processing.user_id
        )
        assert p2 is not None
        assert p2.status == ProcessingStatus.FAILED

    @pytest.mark.asyncio
    async def test_delete_resume(self, kafka_event_handler_fix, create_resume, create_processing, resume_service_fix):
        processing = await create_processing()
        rid = processing.resume_id

        payload = DeleteResume(
            resume_ids=[rid],
            processing_ids=[processing.processing_id],
            requirement_ids=[processing.requirement_id]
        ).model_dump()

        msg = FakeKafkaMessage(
            data=payload,
            key=kafka_event_handler_fix.conf.consumer_keys.delete_resumes,
        )

        await kafka_event_handler_fix.handler_messages(msg)

        with pytest.raises(ResourceNotFound):
            assert await resume_service_fix.get_resume(resume_id=rid, user_id=processing.user_id)

    @pytest.mark.asyncio
    async def test_delete_processing(self, kafka_event_handler_fix, create_processing, processing_service_fix):
        processing = await create_processing()
        pid = processing.processing_id
        rid = processing.resume_id

        payload = DeleteProcessing(
            processing_ids=[pid],
            resume_ids=[rid]
        ).model_dump()

        msg = FakeKafkaMessage(
            data=payload,
            key=kafka_event_handler_fix.conf.consumer_keys.delete_processing,
        )

        await kafka_event_handler_fix.handler_messages(msg)

        with pytest.raises(ResourceNotFound):
            assert await processing_service_fix.get_processing_by_resume(resume_id=rid, user_id=processing.user_id)

    @pytest.mark.asyncio
    async def test_delete_requirements(self, kafka_event_handler_fix, create_resume, create_processing,
                                       requirement_service_fix, resume_service_fix, processing_service_fix):
        # Для удаления требования нужны связанные resume и processing
        resume = await create_resume()
        requirement_id = resume.requirement_id
        user_id = resume.user_id

        processing = await create_processing(user_id)
        pid = processing.processing_id
        rid = resume.resume_id

        payload = DeleteRequirements(
            requirement_ids=[requirement_id],
            resume_ids=[rid],
            processing_ids=[pid],
            user_id=user_id
        ).model_dump()

        msg = FakeKafkaMessage(
            data=payload,
            key=kafka_event_handler_fix.conf.consumer_keys.delete_requirements,
        )

        await kafka_event_handler_fix.handler_messages(msg)

        with pytest.raises(ResourceNotFound):
            assert await requirement_service_fix.get_requirement(requirement_id, resume.user_id)

    @pytest.mark.asyncio
    async def test_message_idempotency(self, kafka_event_handler_fix, user_service_fix):
        # создаём сообщение для нового пользователя и вызываем дважды
        payload = {
            "user_id": 999999,  # уникальный тестовый id, гарантируем отсутствие конфликта
            "username": "idempotent_test@mail.com",
            "full_name": "idempotent",
            "created_at": datetime.now(UTC)
        }

        msg = FakeKafkaMessage(
            data=payload,
            key=kafka_event_handler_fix.conf.consumer_keys.new_user,
        )

        # первый вызов — создаст пользователя
        await kafka_event_handler_fix.handler_messages(msg)
        first = await user_service_fix.get_user(payload["user_id"])
        assert first

        # второй вызов — должен ничего не делать (сообщение помечено в кеше)
        await kafka_event_handler_fix.handler_messages(msg)
        second = await user_service_fix.get_user(payload["user_id"])
        assert second
        # при этом не должно появиться второй записи с другим id — просто проверяем, что пользователь по-прежнему доступен