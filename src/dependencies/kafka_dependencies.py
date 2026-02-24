import asyncio
import json
import os
import time
from datetime import datetime
from typing import AsyncGenerator, List

from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Consumer, KafkaException, KafkaError
import sys

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.database.core import get_db
from src.database.models import User, Resume, Requirements, Processing
from src.dependencies import get_redis
from src.service.config import get_config
from src.service.utils.logger import get_logger
from src.utils import prepare_processing_data

load_dotenv()
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC_CONSUMER_FOR_UPLOADING_DATA = os.getenv('KAFKA_TOPIC_CONSUMER_FOR_UPLOADING_DATA')

admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})\

producer = None # ниже будет переопределён


def create_topic(topic_name, num_partitions=1, replication_factor=1):
    """
    Создаёт топик в Kafka.

    :param topic_name: Название топика
    :param num_partitions: Количество партиций
    :param replication_factor: Фактор репликации
    """
    logger = get_logger(__name__)
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
            'group.id': f'test-group-{time.time()}',  # Уникальный ID
            'auto.offset.reset': 'smallest',
            'enable.auto.commit': False,  # отключаем auto-commit
            'on_commit': self.commit_completed
        } # тут указали в какую функцию попадёт при сохранении
        self.consumer = Consumer(self.conf)
        self.running = True
        self.topic = topic
        self.logger = get_logger(__name__)
        self.conf = get_config()
        check_exists_topic([self.topic])


    # в эту функцию попадём при вызове метода consumer.commit
    def commit_completed(self, err, partitions):
        if err:
            self.logger.error(str(err))
        else:
            self.logger.info("сохранили партию kafka")

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
        self.logger.error(f'Произошла ошибка при обработки сообщения c kafka: {str(e)}')

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
                async with get_redis() as redis:
                    if await redis.get(f"processed_message:{message_id}"):
                        continue

                data = json.loads(msg.value().decode('utf-8'))
                key = msg.key().decode('utf-8')
                await self.worker_topic(data, key, message_id)

                msg_count += 1
                if msg_count % self.conf.min_commit_count_kafka == 0:
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
        try:
            db_gen: AsyncGenerator[AsyncSession, None] = get_db()  # явно указываем тип данных
            db = await db_gen.__anext__()
            db.add(sql_object)
            await db.commit()
        except Exception as e:
            self.logger.error(f"Kafka error: {str(e)}")

    async def new_record_in_redis(self, key, json_str):
        try:
            async with get_redis() as redis:
                await redis.setex(
                    key,
                    self.conf.storage_time_data,
                    json_str
                )
        except Exception as e:
            self.logger.error(f"Ошибка при записи в redis: {str(e)}")

    async def handler_key_new_user(self, data: dict)-> bool:
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
            self.logger.error(f"Kafka New User Error: {str(e)}")
            return False

    async def handler_key_new_resume(self, data: dict) -> bool:
        """Вернёт успех выполнения"""
        self.logger.info("Начали добавлять новое резюме")
        try:
            new_record = Resume(
                resume_id=data['resume_id'],
                user_id=data['user_id'],
                requirement_id=data['requirement_id'],
                resume=data['resume']
            )
            await self.new_record_in_db(new_record)
        except Exception as e:
            self.logger.error(f"ошибка при получении данных о резюме с kafka: {str(e)}")
            return False

        key = f'resume:{new_record.resume_id}'
        json_str = json.dumps(
            {
                'resume_id': new_record.resume_id,
                'user_id': new_record.user_id,
                'requirement_id': new_record.requirement_id,
                'resume': new_record.resume
            }
        )
        await self.new_record_in_redis(key, json_str)

        db_gen: AsyncGenerator[AsyncSession, None] = get_db()
        db = await db_gen.__anext__()

        result = await db.execute(select(Resume).where(Resume.user_id == new_record.requirement_id))
        db_resumes: List[Resume] = result.scalars().all()

        key = f'resume_by_requirement:{new_record.requirement_id}'
        json_str = json.dumps([resume.to_dict() for resume in db_resumes])
        await self.new_record_in_redis(key, json_str)

        return True

    async def handler_key_new_requirements(self, data: dict)-> bool:
        """Вернёт успех выполнения"""
        try:
            new_record = Requirements(
                requirements_id=data['requirements_id'],
                user_id=data['user_id'],
                requirements=data['requirements']
            )
        except Exception as e:
            self.logger.error(f"ошибка при получении данных о требованиях с kafka: {str(e)}")
            return False

        key = f'requirements_by_user:{data['user_id']}'

        async with get_redis() as redis:
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
                    requirements_data = [req.to_dict() for req in db_requirements]  # делаем список из уже имеющихся данных
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
                success=data["success"],
                message_error=data["message_error"],
                wait_seconds=data["wait_seconds"],
                **data["response"]
            )
        except Exception as e:
            self.logger.error(f"ошибка при получении данных о обработке с kafka: {str(e)}")
            return False


        db_gen: AsyncGenerator[AsyncSession, None] = get_db()  # явно указываем тип данных
        db = await db_gen.__anext__()
        result_db = await db.execute(
            select(Processing)
            .where(Processing.processing_id == data["response"]["processing_id"])
        )

        if result_db.scalar_one_or_none():
            self.logger.info(
                f"Processing с id = {data['response']['processing_id']} уже обработали"
            )
            return True

        result_db = await db.execute(
            select(Resume)
            .where(Resume.resume_id == data["response"]["resume_id"])
        )
        if not result_db.scalar_one_or_none():
            self.logger.info(
                f"Processing с id = {data['response']['processing_id']} "
                f"невозможно добавить так как отсутствует указанный Resume = {data["response"]["resume_id"]}"
            )
            return True

        result_db = await db.execute(
            select(Requirements)
            .where(Requirements.requirements_id == data["response"]["requirements_id"])
        )
        if not result_db.scalar_one_or_none():
            self.logger.info(
                f"Processing с id = {data['response']['processing_id']} "
                f"невозможно добавить так как отсутствует указанный Requirements = {data["response"]["requirements_id"]}"
            )
            return True


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

            "success": new_processing.success,
            "message_error": new_processing.message_error,
            "wait_seconds": new_processing.wait_seconds,

            "score": new_processing.score,
            "matches": new_processing.matches,
            "recommendation": new_processing.recommendation,
            "verdict": new_processing.verdict,
            "resume": resume_text,
            "requirements": requirements_text,

            "create_at": new_processing.create_at.isoformat(),
        }

        # ключи для redis
        main_redis_key = f"processing:{data['response']['user_id']}"
        requirements_key = (
            f"processing_requirements:{data['response']['user_id']}:"
            f"{data['response']['requirements_id']}"
        )

        async with get_redis() as redis:
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
                        .where(Processing.user_id == data["response"]['user_id'])
                    )
                    db_processings = result.scalars().all()
                    main_list = [await prepare_processing_data(p) for p in db_processings]  # конвертируем в словарь

                await redis.setex(
                    main_redis_key,
                    self.conf.storage_time_data,
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
                        .where(Processing.user_id == data["response"]['user_id'])
                        .where(Processing.requirements_id == data["response"]['requirements_id'])
                    )
                    db_processings = result.scalars().all()
                    req_list = [await prepare_processing_data(p) for p in db_processings]

                await redis.setex(
                    requirements_key,
                    self.conf.storage_time_data,
                    json.dumps(req_list))

                await db.close()
                return True
            except Exception as e:
                self.logger.error(f"Redis error: {str(e)}")
                await db.close()
                return False

    async def handler_key_delete_resume(self, data: dict) -> bool:
        self.logger.info(f"Запушено удаление резюме. data: {data}")

        db_gen: AsyncGenerator[AsyncSession, None] = get_db()  # явно указываем тип данных
        db = await db_gen.__anext__()

        try:
            # условие на user_id проходить не надо, это должно произойти в другом микросервисе
            await db.execute(delete(Processing).where(Processing.processing_id.in_(data['processing_ids'])))
            await db.execute(delete(Resume).where(Resume.resume_id.in_(data["resume_ids"])))
            await db.commit()
        except Exception as e:
            self.logger.error(f"Ошибка при удалении processing и resume: '{str(e)}'")
            await db.rollback()
            return False

        main_redis_key = f"processing:{data['user_id']}"

        try:
            async with get_redis() as redis:
                main_list_processing = await redis.get(main_redis_key)
                main_new_list_processing = []

                if main_list_processing:
                    main_list_processing = json.loads(main_list_processing) # формируем python объект
                    for processing in main_list_processing:
                        if processing['processing_id'] not in data['processing_ids']: # если данный элемент не надо удалять
                            main_new_list_processing.append(processing)

                    await redis.setex(main_redis_key, self.conf.storage_time_data, json.dumps(main_new_list_processing))

                for requirement_id in data["requirements_ids"]:
                    await redis.delete(f'resume_by_requirement:{requirement_id}')

                    requirements_key = f"processing_requirements:{data['user_id']}:{requirement_id}"
                    requirements_list_processing = await redis.get(requirements_key)
                    requirements_new_list_processing = []

                    if requirements_list_processing:
                        requirements_list_processing = json.loads(requirements_list_processing)

                        for processing in requirements_list_processing:
                            if processing['processing_id'] not in data['processing_ids']:  # если данный элемент не надо удалять
                                requirements_new_list_processing.append(processing)

                        await redis.setex(requirements_key, self.conf.storage_time_data, json.dumps(requirements_new_list_processing))

        except Exception as e:
            self.logger.exception(f'Ошибка в работе redis, при попытке удаления resume: {str(e)}')
            return False

        return True


    async def handler_key_delete_processing(self, data: dict) -> bool:
        self.logger.info(f"Запушено удаление обработки. data: {data}")

        db_gen: AsyncGenerator[AsyncSession, None] = get_db()  # явно указываем тип данных
        db = await db_gen.__anext__()

        try:
            # условие на user_id проходить не надо, это должно произойти в другом микросервисе
            result_db = await db.execute(
                delete(Processing)
                .where(Processing.processing_id.in_(data['processing_ids']))
                .returning(Processing.requirements_id)
            )
            requirements_ids = result_db.scalars().all()
            await db.commit()
        except Exception as e:
            self.logger.error(f"Ошибка при удалении processing: '{str(e)}'")
            await db.rollback()
            return False

        main_redis_key = f"processing:{data['user_id']}"

        try:
            async with get_redis() as redis:
                main_list_processing = await redis.get(main_redis_key)
                main_new_list_processing = []

                if main_list_processing:
                    main_list_processing = json.loads(main_list_processing) # формируем python объект
                    for processing in main_list_processing:
                        if processing['processing_id'] not in data['processing_ids']: # если данный элемент не надо удалять
                            main_new_list_processing.append(processing)

                    await redis.setex(main_redis_key, self.conf.storage_time_data, json.dumps(main_new_list_processing))

                for requirement_id in requirements_ids:
                    requirements_key = f"processing_requirements:{data['user_id']}:{requirement_id}"
                    requirements_list_processing = await redis.get(requirements_key)
                    requirements_new_list_processing = []

                    if requirements_list_processing:
                        requirements_list_processing = json.loads(requirements_list_processing)

                        for processing in requirements_list_processing:
                            if processing['processing_id'] not in data['processing_ids']:  # если данный элемент не надо удалять
                                requirements_new_list_processing.append(processing)

                        await redis.setex(requirements_key, self.conf.storage_time_data, json.dumps(requirements_new_list_processing))

        except Exception as e:
            self.logger.exception(f'Ошибка в работе redis, при попытке удаления processing: {str(e)}')
            return False

        return True

    async def handler_key_delete_requirements(self, data: dict) -> bool:
        # удаление связанных processing
        success = await self.handler_key_delete_processing(data)
        if not success:
            return False

        db_gen: AsyncGenerator[AsyncSession, None] = get_db()  # явно указываем тип данных
        db = await db_gen.__anext__()
        try:
            # удаляем сами requirements
            await db.execute(delete(Processing).where(Processing.processing_id.in_(data['processing_ids'])))
            await db.execute(delete(Resume).where(Resume.resume_id.in_(data['resume_ids'])))
            await db.execute(delete(Requirements).where(Requirements.requirements_id.in_(data['requirements_ids'])))
            await db.commit()
        except Exception as e:
            self.logger.error(f"Ошибка при удалении requirements: '{str(e)}'")
            await db.rollback()
            return False

        async with get_redis() as redis:
            try:
                for requirement_id in data['requirements_ids']:
                    await redis.delete(f'resume_by_requirement:{requirement_id}')

                    requirements_key = f"processing_requirements:{data['user_id']}:{requirement_id}"
                    requirements_list_processing = await redis.get(requirements_key)
                    requirements_new_list_processing = []

                    if requirements_list_processing:
                        requirements_list_processing = json.loads(requirements_list_processing)

                        for processing in requirements_list_processing:
                            if processing['processing_id'] not in data['processing_ids']:  # если данный элемент не надо удалять
                                requirements_new_list_processing.append(processing)

                        await redis.setex(requirements_key, self.conf.storage_time_data, json.dumps(requirements_new_list_processing))

                redis_key = f'requirements_by_user:{data['user_id']}'
                list_requirements = await redis.get(redis_key)

                if list_requirements:
                    list_requirements = json.loads(list_requirements)
                    new_list_requirements = []

                    for requirements in list_requirements:
                        if requirements['requirements_id'] not in data['requirements_ids']: # если не надо удалять
                            new_list_requirements.append(requirements)

                    await redis.setex(redis_key, self.conf.storage_time_data, json.dumps(new_list_requirements))

            except Exception as e:
                self.logger.error(f'Ошибка в работе redis, при попытке удаления requirements: {str(e)}')
                return False

        return True

    async def worker_topic(self, data: dict, key: str, message_id: str):
        success = None
        if key == self.conf.consumer_keys.new_user: # при поступлении нового запроса
            success = await self.handler_key_new_user(data)
        elif key == self.conf.consumer_keys.new_resume:
            success = await self.handler_key_new_resume(data)
        elif key == self.conf.consumer_keys.new_requirements:
            success = await self.handler_key_new_requirements(data)
        elif key == self.conf.consumer_keys.new_processing:
            success = await self.handler_key_new_processing(data)
        elif key == self.conf.consumer_keys.delete_resume:
            success = await self.handler_key_delete_resume(data)
        elif key == self.conf.consumer_keys.delete_processing:
            success = await self.handler_key_delete_processing(data)
        elif key == self.conf.consumer_keys.delete_requirements:
            success = await self.handler_key_delete_requirements(data)

        if success:
            # записываем что сообщение обработано
            async with get_redis() as redis:
                await redis.setex(f"processed_message:{message_id}", self.conf.storage_time_data, '_')
        else:
            self.logger.info(
                f"Не обработали данные полученные от kafka: data = {data}, key = {key}, message_id = {message_id}"
            )

consumer = ConsumerKafkaStorageService(KAFKA_TOPIC_CONSUMER_FOR_UPLOADING_DATA)