import asyncio
from typing import Optional

from src.infrastructure.kafka.consumers.consumer import ConsumerKafka
from src.service.config import get_config
from src.service.kafka import get_kafka_event_handler_service
from src.service.utils.logger import get_logger


class ConsumerRunner:
    def __init__(self, consumer: ConsumerKafka):
        self.consumer = consumer
        self.logger = get_logger(__name__)
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        for _ in range(5):
            try:
                self._task = asyncio.create_task(
                    self.consumer.run_consumer()
                )
                break
            except Exception as e:
                self.logger.warning(f"Cannot start consumer, retrying: {e}")
                await asyncio.sleep(2)

    async def stop(self):
        if not self._task:
            return

        # даём сигнал остановки
        await self.consumer.stop()

        # ждём корректного завершения
        await self._task


async def run_consumer_by_uploading_topic() -> ConsumerRunner:
    conf = get_config()

    consumer = ConsumerKafka(
        topic=conf.env.topic_uploading_data,
        handler_msg_cls=get_kafka_event_handler_service(),
        logger=get_logger(__name__),
        config=conf
    )

    runner = ConsumerRunner(consumer)
    await runner.start()

    return runner