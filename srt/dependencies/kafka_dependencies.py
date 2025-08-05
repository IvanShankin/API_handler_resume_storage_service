import asyncio
import json
import os
from typing import AsyncGenerator

from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Consumer, KafkaException, KafkaError
import sys

from sqlalchemy.ext.asyncio import AsyncSession

from srt.config import logger, MIN_COMMIT_COUNT_KAFKA, KEY_NEW_USER, KEY_NEW_RESUME, KEY_NEW_REQUIREMENTS, \
    KEY_NEW_PROCESSING, MAX_STORAGE_TIME_DATA
from srt.database.database import get_db
from srt.database.models import User, Resume, Requirements, Processing
from srt.dependencies.redis_dependencies import RedisWrapper

load_dotenv()
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC_PRODUCER_FOR_UPLOADING_DATA = os.getenv('KAFKA_TOPIC_PRODUCER_FOR_UPLOADING_DATA')

admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})\

producer = None # ниже будет переопределён
consumer_auth = None # ниже будет переопределён


def create_topic(topic_name, num_partitions=1, replication_factor=1):
    """
    Создаёт топик в Kafka.

    :param topic_name: Название топика
    :param num_partitions: Количество партиций
    :param replication_factor: Фактор репликации
    """
    # Создание объекта топика
    new_topic = NewTopic(
        topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    )

    # Запрос на создание топика
    futures = admin_client.create_topics([new_topic])

    # Ожидание результата
    for topic, future in futures.items():
        try:
            future.result()  # Блокирует выполнение, пока топик не создан
            logger.info(f"Топик '{topic}' успешно создан!")
        except Exception as e:
            logger.error(f"Ошибка при создании топика '{topic}': {e}")


def check_exists_topic(topic_names: list):
    """Проверяет, существует ли топик, если нет, то создаст его"""
    for topic in topic_names:
        cluster_metadata = admin_client.list_topics()
        if not topic in cluster_metadata.topics: # если topic не существует
            create_topic(
                topic_name=topic,
                num_partitions=1,
                replication_factor=1
            )

class ConsumerKafka:
    def __init__(self, topic: str):
        self.conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'foo',
            'auto.offset.reset': 'smallest',
            'enable.auto.commit': False,  # отключаем auto-commit
            'on_commit': self.commit_completed
        } # тут указали в какую функцию попадёт при сохранении
        self.consumer = Consumer(self.conf)
        self.running = True
        self.topic = topic
        check_exists_topic([self.topic])


    # в эту функцию попадём при вызове метода consumer.commit
    def commit_completed(self, err, partitions):
        if err:
            logger.error(str(err))
        else:
            logger.info("сохранили партию kafka")

    async def _get_message_uid(self, msg) -> str:
        """Генерирует уникальный ID сообщения. Для одного и того же сообщения он будет идентичен"""
        return f"{msg.topic()}:{msg.partition()}:{msg.offset()}"

    async def set_running(self, running: bool):
        self.running = running

    async def subscribe_topics(self, topics: list):
        self.consumer.subscribe([topics])
        self.consumer.poll(0)  # форсирует загрузку метаданных

    # ЭТУ ФУНКЦИЮ ПЕРЕОБРЕДЕЛЯЕМ В НАСТЛЕДУЕМОМ КЛАССЕ,
    # ОНА БУДЕТ ВЫПОЛНЯТЬ ДЕЙСТВИЯ ПРИ ПОЛУЧЕНИИ СООБЩЕНИЯ
    async def worker_topic(self, data:dict, key: str):
        pass

    async def error_handler(self, e):
        logger.error(f'Произошла ошибка при обработки сообщения c kafka: {str(e)}')

    async def _run_consumer(self):
        self.consumer.subscribe([self.topic])
        msg_count = 0

        while self.running:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                   (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                data = json.loads(msg.value().decode('utf-8'))
                key = msg.key().decode('utf-8')
                await self.worker_topic(data, key)

                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT_KAFKA == 0:
                    self.consumer.commit(asynchronous=True)

    def consumer_run(self):
        """Создаёт цикл и запускает асинхронный код для чтения сообщений"""
        try:
            loop = asyncio.new_event_loop() # Создаём новый цикл
            asyncio.set_event_loop(loop)  # Назначаем его для текущего потока
            loop.run_until_complete(self._run_consumer()) # Запускаем асинхронный код
        finally:
            self.consumer.close()

# consumer для получения данные о новых запросах
class ConsumerKafkaNotifications(ConsumerKafka):
    def __init__(self, topic: str):
        super().__init__(topic)

    async def worker_topic(self, data: dict, key: str):
        new_record = None
        if key == KEY_NEW_USER: # при поступлении нового запроса
            new_record = User(
                user_id = data['user_id'],
                username = data['username'],
                full_name = data['full_name'],
                created_at = data['created_at']
            )
        elif key == KEY_NEW_RESUME:
            new_record = Resume(
                resume_id = data['resume_id'],
                user_id = data['user_id'],
                resume = data['resume']
            )
            async with RedisWrapper() as redis:
                redis.setex(
                    f'resume:{data['resume_id']}',
                    MAX_STORAGE_TIME_DATA,
                    {'user_id': data['user_id'], 'resume': data['resume']}
                )
        elif key == KEY_NEW_REQUIREMENTS:
            new_record = Requirements(
                requirements_id = data['requirements_id'],
                user_id = data['user_id'],
                requirements = data['requirements']
            )
            async with RedisWrapper() as redis:
                redis.setex(
                    f'requirements:{data['requirements_id']}',
                    MAX_STORAGE_TIME_DATA,
                    {'user_id': data['user_id'], 'requirements': data['requirements']}
                )
        elif key == KEY_NEW_PROCESSING:
            new_record = Processing(
                processing_id = data['processing_id'],
                user_id = data['user_id'],
                resume_id = data['resume_id'],
                requirements_id = data['requirements_id'],
                score = data['score'],
                matches = data['matches'],
                recommendation = data['recommendation'],
                verdict = data['verdict'],
            )
        if new_record:
            try:
                db_gen: AsyncGenerator[AsyncSession, None] = get_db()  # явно указываем тип данных
                db = await db_gen.__anext__()
                db.add(new_record)
                await db.commit()
            except Exception as e:
                logger.error(f"Kafka error: {str(e)}")

consumer_notifications = ConsumerKafkaNotifications(KAFKA_TOPIC_PRODUCER_FOR_UPLOADING_DATA)