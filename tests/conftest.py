import asyncio
import random
import threading
import time
import sys
import inspect
import json
import os
import socket

from datetime import datetime, UTC, timedelta
from dotenv import load_dotenv
from confluent_kafka import Producer, KafkaException
from confluent_kafka.cimpl import NewTopic
from jose import jwt
from sqlalchemy import delete, select, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

load_dotenv()  # Загружает переменные из .env
KAFKA_BOOTSTRAP_SERVERS=os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC_CONSUMER_FOR_UPLOADING_DATA=os.getenv('KAFKA_TOPIC_CONSUMER_FOR_UPLOADING_DATA')
ACCESS_TOKEN_EXPIRE_MINUTES=int(os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES'))
SECRET_KEY=os.getenv('SECRET_KEY')
ALGORITHM=os.getenv('ALGORITHM')
MODE=os.getenv('MODE')

# этот импорт необходимо указывать именно тут для корректной загрузки переменных из .tests.env
import pytest_asyncio

from srt.config import logger
from srt.database.models import User, Resume, Requirements, Processing
from srt.database.database import create_database, get_db as original_get_db, SQL_DB_URL
from srt.dependencies.kafka_dependencies import admin_client, ConsumerKafkaStorageService
from srt.dependencies.redis_dependencies import RedisWrapper


TOPIC_LIST = [
    KAFKA_TOPIC_CONSUMER_FOR_UPLOADING_DATA,
]

# словари для ключей которые должны содержаться в ответе от эндпоинтов /get_processing и /get_processing_detail
DICT_FOR_PROCESSING = {'processing_id', 'resume_id', 'requirements_id', 'user_id', 'create_at', 'score', 'verdict'}
DICT_FOR_PROCESSING_DETAIL = {'processing_id', 'resume_id', 'requirements_id', 'user_id', 'create_at', 'score',
                              'verdict', 'matches', 'recommendation', 'resume', 'requirements'}

async def wait_for(condition_fn, timeout: int = 20, interval: float = 1)-> bool:
    """
    Проверяет результат переданной функции, если он есть, то функция вернёт True.
     Если не успеет за отведённое время, то вызовется ошибка
    :param condition_fn: функция для проверки
    :param timeout: отведенное время в секундах за которое необходимо получить данные с 'condition_fn'
    :param interval: частота вызова функции
    :return:
    """
    start = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() - start < timeout:
        if await condition_fn():
            return True
        await asyncio.sleep(interval)
    raise TimeoutError("Условие не выполнилось за отведённое время")

def create_random_processing(processing_id:int = 1, resume_id:int = 1, requirements_id: int = 1, user_id: int = 1)->dict:
    """Создаёт обработку со случайными данными в следующих ключах: create_at и score
    :return: "processing_id": int,
    "resume_id": int,
    "requirements_id": int,
    "user_id": int,
    "create_at": datetime,
    "score": int,
    "matches": list,
    "recommendation": str,
    "verdict": str,
    "resume": str,
    "requirements": str
    """
    return {
        "processing_id": processing_id,
        "resume_id": resume_id,
        "requirements_id": requirements_id,
        "user_id": user_id,
        "create_at": datetime.fromisoformat(f'2025-07-26 14:{random.randint(10,59)}:37+00'), # рандомные минуты
        "score": random.randint(1,100),
        "matches": ['first_skill'],
        "recommendation": 'recommendation',
        "verdict": 'verdict',
        "resume": 'resume',
        "requirements": 'requirements'
    }


class ProducerKafka:
    def __init__(self):
        self.conf = {
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'client.id': socket.gethostname()
            }
        self.producer = Producer(self.conf)

    def sent_message(self, topic: str, key: str, value: dict):
        try:
            self.producer.produce(topic=topic, key=key, value=json.dumps(value).encode('utf-8'), callback=self._acked)
            self.producer.flush()
            self.producer.poll(1)
        except KafkaException as e:
            logger.error(f"Kafka error: {e}")

    def _acked(self, err, msg):
        logger.info(f"Kafka new message: err: {err}\nmsg: {msg.value().decode('utf-8')}")

conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'test-group-' + str(os.getpid()),  # Уникальный group.id для каждого запуска тестов
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': 'false',  # Отключаем авто-коммит
    'isolation.level': 'read_committed'
}

producer = ProducerKafka()

@pytest_asyncio.fixture(scope="function")
async def engine():
    if MODE != "TEST":
        raise Exception("Используется основная БД!")

    await create_database()


# Мок-версия get_db
async def _mock_get_db():
    """Переопределяет функцию get_db. Отличия: каждый раз создаёт новый engine и session_local"""
    engine = create_async_engine(SQL_DB_URL)
    session_local = sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False
    )
    db = session_local()
    try:
        yield db
    finally:
        await db.close()


@pytest_asyncio.fixture(autouse=True, scope="session")
def override_get_db_globally():
    original = original_get_db # сохранение оригинальной функции
    patched_modules = [] # хранит пути где переопределили get_db

    # поиск по всем модулям
    for module_name, module in list(sys.modules.items()):
        if not module:
            continue
        # фильтруем только свои модули
        if not module_name.startswith("srt."):
            continue
        try:
            for attr_name, attr_value in inspect.getmembers(module):
                if attr_value is original_get_db: # если значение атрибута это оригинальная get_db
                    setattr(module, attr_name, _mock_get_db) # замена на новую get_db
                    patched_modules.append((module, attr_name))
        except Exception:
            # если модуль ломается при доступе к атрибутам — пропускаем
            continue

    # подмена в FastAPI dependency_overrides
    try:
        from srt.main import app
        app.dependency_overrides[original_get_db] = _mock_get_db
    except ImportError:
        pass
    yield

    # откат
    for module, attr_name in patched_modules:
        setattr(module, attr_name, original)

    try:
        from srt.main import app
        app.dependency_overrides.clear()
    except ImportError:
        pass

@pytest_asyncio.fixture
async def db_session() -> AsyncSession:
    """Соединение с БД"""
    from srt.database.database import get_db  # Импортируем после переопределения

    db_gen = get_db()
    session = await db_gen.__anext__()
    try:
        yield session
    finally:
        await session.close()

@pytest_asyncio.fixture(scope="function", autouse=True)
async def clearing_db(db_session: AsyncSession):
    """Очищает базу банных"""
    await db_session.execute(delete(Processing))
    await db_session.execute(delete(Requirements))
    await db_session.execute(delete(Resume))
    await db_session.execute(delete(User))
    await db_session.commit()

@pytest_asyncio.fixture(scope="function", autouse=True)
async def clearing_redis():
    """Очищает redis"""
    async with RedisWrapper() as redis:
        await redis.flushdb()

@pytest_asyncio.fixture(scope='session', autouse=True)
async def check_kafka_connection():
    try:
        admin_client.list_topics(timeout=10)
    except Exception:
        raise Exception("Не удалось установить соединение с Kafka!")

@pytest_asyncio.fixture(scope='function', autouse=True)
async def start_kafka_consumer():
    """Фикстура для запуска нового consumer Kafka для каждого теста"""
    consumer_instance = ConsumerKafkaStorageService(KAFKA_TOPIC_CONSUMER_FOR_UPLOADING_DATA)
    consumer_thread = threading.Thread(target=consumer_instance.consumer_run)
    consumer_thread.daemon = True
    consumer_thread.start()
    await asyncio.sleep(2)
    yield

    # Остановить consumer после завершения теста (необязательно, но желательно)
    await consumer_instance.set_running(False)

@pytest_asyncio.fixture(scope='function')
async def clearing_kafka():
    """Очищает топик у kafka с которым работаем, путём его пересоздания"""
    max_retries = 15

    for topic in TOPIC_LIST:
        admin_client.delete_topics([topic])

    # Ждём подтверждения удаления
    for _ in range(max_retries):
        meta = admin_client.list_topics(timeout=5)
        if all(t not in meta.topics for t in TOPIC_LIST):
            break
        time.sleep(1)
    else:
        logger.warning(f"Топик всё ещё существует после попыток удаления.")

    # Создаём топики
    for topic in TOPIC_LIST:
        admin_client.create_topics([NewTopic(topic=topic, num_partitions=1, replication_factor=1)])

    time.sleep(5)

    # Ждём инициализации partition и leader
    for _ in range(max_retries):
        meta_data = admin_client.list_topics(timeout=5)
        ready = True
        for topic in TOPIC_LIST:
            topic_meta = meta_data.topics.get(topic)
            if not topic_meta or topic_meta.error:
                ready = False
                break
            partitions = topic_meta.partitions
            if 0 not in partitions or partitions[0].leader == -1:
                ready = False
                break
        if ready:
            break
        time.sleep(1)
    else:
        raise RuntimeError("Partition или leader не инициализирован после создания топика.")

@pytest_asyncio.fixture(scope="function")
async def create_user(db_session)->dict:
    """
    Создаёт юзера в БД
    :return: dict {"user_id": int, "access_token": str, "username": str, "full_name": str, "created_at": datetime(UTC)}
    """
    await db_session.execute(delete(User))
    await db_session.commit()

    max_id_result = await db_session.execute(select(func.max(User.user_id)))
    next_id = (max_id_result.scalar() or 0) + 1

    new_user = User(
        user_id=next_id,
        username='test_username',
        full_name='test_full_name',
        created_at=datetime.now(UTC),
    )
    db_session.add(new_user)
    await db_session.commit()
    await db_session.refresh(new_user)

    to_encode = {"sub": str(new_user.user_id)}.copy()

    # Установка времени истечения токена
    expire = datetime.now(UTC) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    # Добавляем поле с временем истечения
    to_encode.update({"exp": expire})

    # Кодируем данные в JWT токен
    access_token = jwt.encode(
        to_encode,  # Данные для кодирования
        SECRET_KEY,  # Секретный ключ из конфига
        algorithm=ALGORITHM  # Алгоритм шифрования
    )

    return {
        "user_id": new_user.user_id,
        'access_token': access_token,
        "username": new_user.username,
        "full_name": new_user.full_name,
        "created_at": new_user.created_at
    }

@pytest_asyncio.fixture(scope="function")
async def create_resume(db_session, create_user)->dict:
    """
    Создаёт резюме в БД
    :return: dict {"resume_id": int, "user_id": int, "access_token": str, "resume": str}
    """
    max_id_result = await db_session.execute(select(func.max(Resume.resume_id)))
    next_id = (max_id_result.scalar() or 0 ) + 1 # определяем следующий свободный id

    new_resume = Resume(
        resume_id=next_id,
        user_id=create_user['user_id'],
        resume='test_resume',
    )
    db_session.add(new_resume)
    await db_session.commit()
    await db_session.refresh(new_resume)

    return {
        "resume_id": new_resume.resume_id,
        "user_id": new_resume.user_id,
        'access_token': create_user['access_token'],
        "resume": new_resume.resume
    }

@pytest_asyncio.fixture(scope="function")
async def create_requirements(db_session, create_user)->dict:
    """
    Создаёт требование в БД
    :return: dict {"requirements_id": int, "user_id": int, "access_token": str, "requirements": str}
    """
    max_id_result = await db_session.execute(select(func.max(Requirements.requirements_id)))
    next_id = (max_id_result.scalar() or 0 ) + 1 # определяем следующий свободный id

    new_requirements = Requirements(
        requirements_id=next_id,
        user_id=create_user['user_id'],
        requirements='test_requirements',
    )
    db_session.add(new_requirements)
    await db_session.commit()
    await db_session.refresh(new_requirements)

    return {
        "requirements_id": new_requirements.requirements_id,
        "user_id": new_requirements.user_id,
        'access_token': create_user['access_token'],
        "requirements": new_requirements.requirements
    }


@pytest_asyncio.fixture(scope="function")
async def create_resume_and_requirements(db_session, create_user)->dict:
    """
    Создаёт резюме и требование c одним и тем же пользователем в БД
    :return: dict {"requirements_id": int, "resume_id": int, "user_id": int, "access_token": str, "requirements": str, "resume": str,}
    """
    new_requirements = Requirements(
        requirements_id=1,
        user_id=create_user['user_id'],
        requirements='test_requirements',
    )
    new_resume = Resume(
        resume_id=1,
        user_id=create_user['user_id'],
        resume='test_resume',
    )

    db_session.add(new_requirements)
    db_session.add(new_resume)
    await db_session.commit()
    await db_session.refresh(new_requirements)
    await db_session.refresh(new_resume)

    return {
        "resume_id": new_resume.resume_id,
        "requirements_id": new_requirements.requirements_id,
        "user_id": new_resume.user_id,
        'access_token': create_user['access_token'],
        "requirements": new_requirements.requirements,
        "resume": new_resume.resume
    }


async def create_processing(
        db_session: AsyncSession,
        user_id: int,
        requirements_id: int
) -> dict:
    """
    Создаёт обработку (Processing) в БД, проверяя и создавая при необходимости Requirements.
    Всегда создаёт новое Resume перед созданием Processing.

    :param db_session: Асинхронная сессия БД
    :param user_id: ID пользователя
    :param requirements_id: ID требований (если не существует - будут созданы новые)
    :return: dict {
        "processing_id": int,
        "resume_id": int,
        "requirements_id": int,
        "user_id": int,
        "create_at": datetime,
        "score": int,
        "matches": list,
        "recommendation": str,
        "verdict": str,
        "resume": str,
        "requirements": str
    }
    """
    # проверяем существование Requirements если нет, то создаём новые
    requirements = await db_session.execute(
        select(Requirements).where(Requirements.requirements_id == requirements_id)
    )
    requirements = requirements.scalar_one_or_none()

    if not requirements:
        # создаём новые требования
        requirements = Requirements(
            requirements_id=requirements_id,
            user_id=user_id,
            requirements='test_requirements',
        )
        db_session.add(requirements)
        await db_session.commit()
        await db_session.refresh(requirements)

    # создаём новое резюме
    max_resume_id = await db_session.execute(select(func.max(Resume.resume_id)))
    new_resume_id = (max_resume_id.scalar() or 0) + 1

    new_resume = Resume(
        resume_id=new_resume_id,
        user_id=user_id,
        resume='test_resume',
    )
    db_session.add(new_resume)
    await db_session.commit()
    await db_session.refresh(new_resume)

    # создаём обработку
    max_processing_id = await db_session.execute(select(func.max(Processing.processing_id)))
    new_processing_id = (max_processing_id.scalar() or 0) + 1

    processing_data = create_random_processing(
        processing_id=new_processing_id,
        resume_id=new_resume.resume_id,
        requirements_id=requirements.requirements_id,
        user_id=user_id
    )

    new_processing = Processing(
        processing_id=processing_data['processing_id'],
        resume_id=processing_data['resume_id'],
        requirements_id=processing_data['requirements_id'],
        user_id=processing_data['user_id'],
        create_at=processing_data['create_at'],
        score=processing_data['score'],
        matches=processing_data['matches'],
        recommendation=processing_data['recommendation'],
        verdict=processing_data['verdict'],
    )

    db_session.add(new_processing)
    await db_session.commit()
    await db_session.refresh(new_processing)

    return {
        "processing_id": new_processing.processing_id,
        "resume_id": new_resume.resume_id,
        "requirements_id": requirements.requirements_id,
        "user_id": user_id,
        "create_at": processing_data['create_at'].strftime("%Y-%m-%d %H:%M:%S%z").replace(' ', 'T').replace('+0000', 'Z') ,
        "score": processing_data['score'],
        "matches": processing_data['matches'],
        "recommendation": processing_data['recommendation'],
        "verdict": processing_data['verdict'],
        "resume": new_resume.resume,
        "requirements": requirements.requirements,
    }