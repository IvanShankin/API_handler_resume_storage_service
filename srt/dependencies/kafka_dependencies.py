import asyncio
import json
import os
from datetime import datetime, timezone
from sys import exception
from typing import AsyncGenerator

from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Consumer, KafkaException, KafkaError
import sys

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from srt.config import logger, MIN_COMMIT_COUNT_KAFKA, KEY_NEW_USER, KEY_NEW_RESUME, KEY_NEW_REQUIREMENTS, \
    KEY_NEW_PROCESSING, STORAGE_TIME_DATA
from srt.database.database import get_db
from srt.database.models import User, Resume, Requirements, Processing
from srt.dependencies.redis_dependencies import RedisWrapper
from srt.requests.get import prepare_processing_data

load_dotenv()
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC_CONSUMER_FOR_UPLOADING_DATA = os.getenv('KAFKA_TOPIC_CONSUMER_FOR_UPLOADING_DATA')

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
    async def worker_topic(self, data:dict, key: str, message_id: str):
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
                # проверка на обработанное сообщение
                message_id = await self._get_message_uid(msg)
                async with RedisWrapper() as redis:
                    if await redis.get(f"processed_message:{message_id}"):
                        continue

                data = json.loads(msg.value().decode('utf-8'))
                key = msg.key().decode('utf-8')
                await self.worker_topic(data, key, message_id)

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
class ConsumerKafkaStorageService(ConsumerKafka):
    def __init__(self, topic: str):
        super().__init__(topic)

    async def new_record_in_db(self, sql_object):
        """Вернёт успех выполнения"""
        try:
            db_gen: AsyncGenerator[AsyncSession, None] = get_db()  # явно указываем тип данных
            db = await db_gen.__anext__()
            db.add(sql_object)
            await db.commit()
        except Exception as e:
            logger.error(f"Kafka error: {str(e)}")

    async def new_record_in_redis(self, key, json_str):
        """Вернёт успех выполнения"""
        try:
            async with RedisWrapper() as redis:
                await redis.setex(
                    key,
                    STORAGE_TIME_DATA,
                    json_str
                )
        except Exception as e:
            logger.error(f"Ошибка при записи в redis: {str(e)}")

    async def handler_key_new_user(self, data: dict):
        """Вернёт успех выполнения"""
        try:
            new_user = User(
                user_id=data['user_id'],
                username=data['username'],
                full_name=data['full_name'],
                created_at=datetime.fromisoformat(data['created_at'])
            )
            await self.new_record_in_db(new_user)
            return True
        except Exception as e:
            logger.error(f"Kafka New User Error: {str(e)}")
            return False

    async def handler_key_new_resume(self, data: dict):
        """Вернёт успех выполнения"""
        try:
            new_record = Resume(
                resume_id=data['resume_id'],
                user_id=data['user_id'],
                resume=data['resume']
            )
            await self.new_record_in_db(new_record)
        except Exception as e:
            logger.error(f"ошибка при получении данных о резюме с kafka: {str(e)}")
            return False

        key = f'resume:{data['resume_id']}'
        json_str = json.dumps({'resume_id': data['resume_id'], 'user_id': data['user_id'], 'resume': data['resume']})
        await self.new_record_in_redis(key, json_str)

        return True

    async def handler_key_new_requirements(self, data: dict):
        """Вернёт успех выполнения"""
        try:
            new_record = Requirements(
                requirements_id=data['requirements_id'],
                user_id=data['user_id'],
                requirements=data['requirements']
            )
        except Exception as e:
            logger.error(f"ошибка при получении данных о требованиях с kafka: {str(e)}")
            return False

        key = f'requirements:{data['user_id']}'

        async with RedisWrapper() as redis:
            data_redis = await redis.get(key)

            if data_redis:  # если данные в redis имеются
                data_redis = json.loads(data_redis)
                data_redis.append({
                    'requirements_id': data['requirements_id'],
                    'user_id': data['user_id'],
                    'requirements': data['requirements']
                })
                json_str = json.dumps(data_redis)
            else:  # если данных в redis нет -> ищем в БД
                db_gen: AsyncGenerator[AsyncSession, None] = get_db()  # явно указываем тип данных
                db = await db_gen.__anext__()

                result = await db.execute(select(Requirements).where(Requirements.user_id == data['user_id']))
                db_requirements = result.scalars().all()

                if db_requirements:
                    # Подготовка данных для кэша
                    requirements_data = [req.to_dict() for req in
                                         db_requirements]  # делаем список из уже имеющихся данных
                    requirements_data.append(data)  # Можем сохранять data т.к. она имеет уже необходимую структуру
                else:
                    requirements_data = [data]

                json_str = json.dumps(requirements_data)

            await self.new_record_in_db(new_record)
            await self.new_record_in_redis(key, json_str)

            return True

    async def handler_key_new_processing(self, data: dict)-> bool:
        """Вернёт успех выполнения"""
        try:
            new_processing = Processing(
                processing_id=data['processing_id'],
                resume_id=data['resume_id'],
                requirements_id=data['requirements_id'],
                user_id=data['user_id'],
                create_at=datetime.now(timezone.utc),
                score=data['score'],
                matches=data['matches'],
                recommendation=data['recommendation'],
                verdict=data['verdict'],
            )
        except Exception as e:
            logger.error(f"ошибка при получении данных о обработке с kafka: {str(e)}")
            return False

        db_gen: AsyncGenerator[AsyncSession, None] = get_db()  # явно указываем тип данных
        db = await db_gen.__anext__()
        db.add(new_processing)
        await db.commit()

        # запрос в БД для получения resume и requirements
        result = await db.execute(
            select(Processing)
            .options(
                selectinload(Processing.resume),
                selectinload(Processing.requirements)
            )
            .where(Processing.processing_id == new_processing.processing_id)
        )
        new_processing_full = result.scalar_one()

        # получаем связанные данные (резюме и требования)
        resume_text = new_processing_full.resume.resume if new_processing_full.resume else None
        requirements_text = new_processing_full.requirements.requirements if new_processing_full.requirements else None

        # подготовка данных для Redis
        processing_data = {
            "processing_id": new_processing.processing_id,
            "resume_id": new_processing.resume_id,
            "requirements_id": new_processing.requirements_id,
            "user_id": new_processing.user_id,
            "create_at": new_processing.create_at.isoformat(),
            "score": new_processing.score,
            "matches": new_processing.matches,
            "recommendation": new_processing.recommendation,
            "verdict": new_processing.verdict,
            "resume": resume_text,
            "requirements": requirements_text
        }

        # ключи для redis
        main_redis_key = f"processing:{data['user_id']}"
        requirements_key = f"processing_requirements:{data['user_id']}:{data['requirements_id']}"

        async with RedisWrapper() as redis:
            try:
                # обновляем основной кеш (все обработки пользователя)
                main_data = await redis.get(main_redis_key)
                if main_data:
                    main_list = json.loads(main_data)
                    main_list.append(processing_data)
                else:
                    # если данных нет в Redis, получаем из БД
                    result = await db.execute(
                        select(Processing)
                        .where(Processing.user_id == data['user_id'])
                    )
                    db_processings = result.scalars().all()
                    main_list = [await prepare_processing_data(p) for p in db_processings]  # конвертируем в словарь

                await redis.setex(
                    main_redis_key,
                    STORAGE_TIME_DATA,
                    json.dumps(main_list)
                )
                # обновляем кеш для конкретного требования
                requirements_data = await redis.get(requirements_key)
                if requirements_data:
                    req_list = json.loads(requirements_data)
                    req_list.append(processing_data)
                else:
                    # если данных нет в Redis, получаем из БД
                    result = await db.execute(
                        select(Processing)
                        .where(Processing.user_id == data['user_id'])
                        .where(Processing.requirements_id == data['requirements_id'])
                    )
                    db_processings = result.scalars().all()
                    req_list = [await prepare_processing_data(p) for p in db_processings]

                await redis.setex(
                    requirements_key,
                    STORAGE_TIME_DATA,
                    json.dumps(req_list))

                return True
            except Exception as e:
                logger.error(f"Redis error: {str(e)}")
                return False

    async def worker_topic(self, data: dict, key: str, message_id: str):
        success = None
        if key == KEY_NEW_USER: # при поступлении нового запроса
            success = await self.handler_key_new_user(data)
        elif key == KEY_NEW_RESUME:
            success = await self.handler_key_new_resume(data)
        elif key == KEY_NEW_REQUIREMENTS:
            success = await self.handler_key_new_requirements(data)
        elif key == KEY_NEW_PROCESSING:
            success = await self.handler_key_new_processing(data)


        if success:
            # записываем что сообщение обработано
            async with RedisWrapper() as redis:
                await redis.setex(f"processed_message:{message_id}", STORAGE_TIME_DATA, '_')

consumer_notifications = ConsumerKafkaStorageService(KAFKA_TOPIC_CONSUMER_FOR_UPLOADING_DATA)