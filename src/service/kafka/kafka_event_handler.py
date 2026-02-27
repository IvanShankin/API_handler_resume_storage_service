from logging import Logger

from orjson import orjson

from src.database.models import ProcessingStatus
from src.exeptions.service_exc import IDAlreadyExists, InsertionErrorService
from src.repository.redis.kafka_message_cache import KafkaMessageCacheRepository
from src.schemas.kafka_data import NewUser, EndProcessing, DeleteProcessing, NewResume, DeleteResume, NewRequirement, \
    DeleteRequirements
from src.service.config.schemas import Config
from src.service.processing.processing_service import ProcessingService
from src.service.requirements.requirements_service import RequirementService
from src.service.resumes.resumes_service import ResumeService
from src.service.users import UsersService


class KafkaEventHandlerService:

    def __init__(
        self,
        user_service: UsersService,
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

    def _get_message_uid(self, msg) -> str:
        return f"{msg.topic}:{msg.partition}:{msg.offset}"

    async def _handler_by_key(self, data: dict, key: str, message_id: str):

        if key == self.conf.consumer_keys.new_user:
            new_user_data = NewUser(**data)
            try:
                await self.user_service.create_user(**(new_user_data.model_dump()))
            except IDAlreadyExists:
                self.logger.warning(
                    f"пользователь с данным ID = {new_user_data.user_id} уже имеется и не будет добавлен"
                )

        elif key == self.conf.consumer_keys.new_resume:
            new_resume_data = NewResume(**data)
            try:
                await self.resume_service.create_resume(**(new_resume_data.model_dump()))
            except InsertionErrorService:
                self.logger.warning(
                    f"Резюме с данным ID = {new_resume_data.resume_id} не будет добавлен. "
                    f"Оно либо уже имеется, либо отсутствуют данные на которое оно ссылается"
                )

        elif key == self.conf.consumer_keys.new_requirements:
            new_requirement_data = NewRequirement(**data)
            try:
                await self.requirement_service.create_requirement(**(new_requirement_data.model_dump()))
            except IDAlreadyExists:
                self.logger.warning(
                    f"Требование с ID = {new_requirement_data.requirement_id} уже имеется и не будет добавлено"
                )

        elif key == self.conf.consumer_keys.end_processing:
            new_user_data = EndProcessing(
                status=ProcessingStatus.SUCCESSFULLY if data["success"] else  ProcessingStatus.FAILED,
                **data
            )
            await self.processing_service.update_processing(**(new_user_data.model_dump()))

        elif key == self.conf.consumer_keys.delete_resumes:
            data_for_deleting = DeleteResume(**data)
            await self.resume_service.delete_resume(**(data_for_deleting.model_dump()))

        elif key == self.conf.consumer_keys.delete_processing:
            data_for_deleting = DeleteProcessing(**data)
            await self.processing_service.delete_processing(**(data_for_deleting.model_dump()))

        elif key == self.conf.consumer_keys.delete_requirements:
            data_for_deleting = DeleteRequirements(**data)
            await self.requirement_service.delete_requirement(**(data_for_deleting.model_dump()))

        # записываем что сообщение обработано
        await self.kafka_message_cache.set(message_id)

    async def handler_messages(self, msg):
        """
        :return: Успех обработки. Если уже было обработанно ранее, то вернёт False
        """
        message_id = self._get_message_uid(msg)

        if await self.kafka_message_cache.get(message_id):
            return

        data = orjson.loads(msg.value.decode("utf-8"))
        key = (
            msg.key.decode("utf-8")
            if msg.key
            else None
        )

        await self._handler_by_key(data, key, message_id)

