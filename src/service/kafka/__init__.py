from logging import Logger
from fastapi import Depends

from src.repository.redis import KafkaMessageCacheRepository, get_kafka_message_cache_repository
from src.service.config import get_config
from src.service.config.schemas import Config
from src.service.kafka.kafka_event_handler import KafkaEventHandlerService
from src.service.processing import ProcessingService, get_processing_service
from src.service.requirements import get_requirement_service
from src.service.requirements.requirements_service import RequirementService
from src.service.resumes import get_resume_service, ResumeService
from src.service.users import UsersService, get_users_service
from src.service.utils.logger import get_logger


async def get_kafka_event_handler_service(
    user_service: UsersService = Depends(get_users_service),
    requirement_service: RequirementService = Depends(get_requirement_service),
    resume_service: ResumeService = Depends(get_resume_service),
    processing_service: ProcessingService = Depends(get_processing_service),
    kafka_message_cache: KafkaMessageCacheRepository = Depends(get_kafka_message_cache_repository),
    logger: Logger = Depends(get_logger),
    conf: Config = Depends(get_config),
) -> KafkaEventHandlerService:
    return KafkaEventHandlerService(
        user_service=user_service,
        requirement_service=requirement_service,
        resume_service=resume_service,
        processing_service=processing_service,
        kafka_message_cache=kafka_message_cache,
        logger=logger,
        config=conf,
    )

