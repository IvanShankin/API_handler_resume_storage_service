from aiokafka.admin import NewTopic

from src.infrastructure.kafka.admin_client import get_admin_client
from src.service.utils.logger import get_logger


async def create_topic(
    topic_name: str,
    num_partitions: int = 1,
    replication_factor: int = 1
):
    logger = get_logger(__name__)

    try:
        admin = await get_admin_client()
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )
        await admin.create_topics([topic])
        logger.info(f"Topic created: {topic_name}")

    except Exception as e:
        if "already exists" in str(e).lower():
            logger.info(f"Topic already exists: {topic_name}")
        else:
            logger.exception(f"Topic creation error: {e}")


async def check_exists_topic(topic_name: str):
    """Проверит наличие топика, если его нет, то создаст"""
    admin = await get_admin_client()
    metadata = await admin.list_topics()

    if topic_name not in metadata:
        await create_topic(topic_name)

    get_logger(__name__).info(f"Topic already exists: {topic_name}")
