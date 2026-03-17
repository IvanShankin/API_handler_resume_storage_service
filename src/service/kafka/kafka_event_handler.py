from logging import Logger

from orjson import orjson

from src.database.models import ProcessingStatus
from src.exeptions.service_exc import IDAlreadyExists, InsertionErrorService
from src.repository.redis.kafka_message_cache import KafkaMessageCacheRepository
from src.schemas.kafka_data import NewUser, DeleteProcessing, NewResume, DeleteResume, NewRequirement, \
    DeleteRequirements, EndProcessingForFunc, NewProcessing
from src.service.config.schemas import Config
from src.service.processing.processing_service import ProcessingService
from src.service.requirements.requirements_service import RequirementService
from src.service.resumes.resumes_service import ResumeService
from src.service.users import UserService


class KafkaEventHandlerService:

    def __init__(
        self,
        user_service: UserService,
        requirement_service: RequirementService,
        resume_service: ResumeService,
        processing_service: ProcessingService,
        kafka_message_cache: KafkaMessageCacheRepository,
        logger: Logger,
        config: Config
    ):
        self.user_service = user_service
        self.requirement_service = requirement_service
        self.resume_service = resume_service
        self.processing_service = processing_service
        self.kafka_message_cache = kafka_message_cache
        self.logger = logger
        self.conf = config

        # Обработчики для каждого топика
        self.handlers = {
            self.conf.kafka_topics.user_created: self._user_created,
            self.conf.kafka_topics.resume_created: self._resume_created,
            self.conf.kafka_topics.requirements_created: self._requirement_created,
            self.conf.kafka_topics.processing_created: self._processing_created,
            self.conf.kafka_topics.processing_finished: self._processing_finished,

            self.conf.kafka_topics.resumes_deleted: self._resumes_deleted,
            self.conf.kafka_topics.processing_deleted: self._processing_deleted,
            self.conf.kafka_topics.requirements_deleted: self._requirements_deleted,
        }

    def _get_message_uid(self, msg) -> str:
        return f"{msg.topic}:{msg.partition}:{msg.offset}"

    async def _user_created(self, data: dict):
        new_user_data = NewUser(**data)
        try:
            await self.user_service.create_user(**(new_user_data.model_dump()))
            self.logger.info(f"Создан пользователь. ID={new_user_data.user_id}")
        except IDAlreadyExists:
            self.logger.warning(
                f"пользователь с данным ID = {new_user_data.user_id} уже имеется и не будет добавлен"
            )

    async def _resume_created(self, data: dict):
        new_resume_data = NewResume(**data)
        try:
            await self.resume_service.create_resume(**(new_resume_data.model_dump()))
            self.logger.info(f"Создано резюме. ID={new_resume_data.resume_id}")
        except InsertionErrorService:
            self.logger.warning(
                f"Резюме с данным ID = {new_resume_data.resume_id} не будет добавлен. "
                f"Оно либо уже имеется, либо отсутствуют данные на которое оно ссылается"
            )

    async def _requirement_created(self, data: dict):
        new_requirement_data = NewRequirement(**data)
        try:
            await self.requirement_service.create_requirement(**(new_requirement_data.model_dump()))
            self.logger.info(f"Создано требование. ID={new_requirement_data.requirement_id}")
        except IDAlreadyExists:
            self.logger.warning(
                f"Требование с ID = {new_requirement_data.requirement_id} уже имеется и не будет добавлено"
            )


    async def _processing_created(self, data: dict):
        new_processing = NewProcessing(**data)
        try:
            await self.processing_service.create_processing(
                processing_id=new_processing.processing_id,
                resume_id=new_processing.resume_id,
                requirement_id=new_processing.requirement_id,
                user_id=new_processing.user_id,
            )
            self.logger.info(f"Создана обработка. ID={new_processing.processing_id}")
        except InsertionErrorService:
            self.logger.warning(
                f"Обработка с данным ID = {new_processing.processing_id} не будет добавлен. "
                f"Оно либо уже имеется, либо отсутствуют данные на которое оно ссылается"
            )

    async def _processing_finished(self, data: dict):
        new_user_data = EndProcessingForFunc(
            status=ProcessingStatus.SUCCESSFULLY if data["success"] else ProcessingStatus.FAILED,
            **data
        )
        await self.processing_service.update_processing(**(new_user_data.model_dump()))
        self.logger.info(f"обновлена обработка. ID={new_user_data.processing_id}")

    async def _resumes_deleted(self, data: dict):
        data_for_deleting = DeleteResume(**data)
        await self.resume_service.delete_resume(**(data_for_deleting.model_dump()))
        self.logger.info(f"Удалено резюме. IDs={data_for_deleting.resume_ids}")

    async def _processing_deleted(self, data: dict):
        data_for_deleting = DeleteProcessing(**data)
        await self.processing_service.delete_processing(**(data_for_deleting.model_dump()))
        self.logger.info(f"Удалена обработка. IDs={data_for_deleting.processing_ids}")

    async def _requirements_deleted(self, data: dict):
        data_for_deleting = DeleteRequirements(**data)
        await self.requirement_service.delete_requirement(**(data_for_deleting.model_dump()))
        self.logger.info(f"Удалено требование. IDs={data_for_deleting.requirement_ids}")

    async def handler_messages(self, msg):
        """
        :return: Успех обработки. Если уже было обработанно ранее, то вернёт False
        """
        self.logger.info(f"Получено новое сообщение от kafka")
        message_id = self._get_message_uid(msg)

        if await self.kafka_message_cache.get(message_id):
            return

        data = orjson.loads(msg.value.decode("utf-8"))
        key = (
            msg.key.decode("utf-8")
            if msg.key
            else None
        )

        try:
            handler = self.handlers.get(msg.topic)
            if handler:
                await handler(data)
            else:
                self.logger.warning(f"Не нашли топик из config для сообщения. Топик: {msg.topic}")

            # записываем что сообщение обработано
            await self.kafka_message_cache.set(message_id)
        except Exception:
            self.logger.exception(
                f"Обработка сообщения из kafka завершилась с ошибкой. Данные о сообщении: \n"
                f"data: {data}\n"
                f"key: {key}\n"
                f"message_id: {message_id}\n\n"
                "Ошибка: "
            )
