import asyncio
import time
from logging import Logger
from typing import Any, List

from aiokafka import AIOKafkaConsumer

from src.service.config.schemas import Config


class ConsumerKafka:
    def __init__(self, topics: List[str], handler_msg_cls: Any, logger: Logger, config: Config):
        """
        :param topics: Топик по которому будет слушать
        :param handler_msg_cls: Любой экземпляр класса имеющий асинхронный метод `handler_messages`
        """
        self.topics = topics
        self.handler_msg_cls = handler_msg_cls
        self.running = True
        self.logger = logger
        self.conf = config

        self._stop_event = asyncio.Event()

        self.consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=self.conf.env.kafka_bootstrap_servers,
            group_id=f"storage-service-group",
            retry_backoff_ms=2000,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )

    async def error_handler(self, e: Exception):
        self.logger.error(
            f"Ошибка при обработке сообщения из Kafka: {str(e)}"
        )

    async def _start_consumer(self):
        while True:
            try:
                await self.consumer.start()
                self.logger.info("Успешно подключились к Kafka")
            except Exception:
                self.logger.exception("Неудачная попытка запуска Kafka Consumer")
                await asyncio.sleep(5)

    async def run_consumer(self):
        await self._start_consumer()
        msg_count = 0

        try:
            while not self._stop_event.is_set():
                try:
                    msg = await asyncio.wait_for(
                        self.consumer.getone(),
                        timeout=1.0,  # важно для возможности graceful stop
                    )
                except asyncio.TimeoutError:
                    continue

                try:
                    await self.handler_msg_cls.handler_messages(msg)
                    msg_count += 1

                    if msg_count % self.conf.min_commit_count_kafka == 0:
                        await self.consumer.commit()
                        self.logger.info("Batch commit выполнен")

                except Exception as e:
                    await self.error_handler(e)

        finally:
            # финальный commit
            try:
                await self.consumer.commit()
            except Exception:
                pass

            await self.consumer.stop()
            self.logger.info("Kafka consumer корректно остановлен")

    async def stop(self):
        self._stop_event.set()


