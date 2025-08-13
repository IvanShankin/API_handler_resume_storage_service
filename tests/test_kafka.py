import asyncio
import json

import pytest
from sqlalchemy import select

from srt.config import KEY_NEW_USER, KEY_NEW_RESUME, KEY_NEW_REQUIREMENTS, KEY_NEW_PROCESSING, KEY_DELETE_PROCESSING, \
    KEY_DELETE_REQUIREMENTS
from srt.database.models import User, Resume, Requirements, Processing
from srt.dependencies.redis_dependencies import RedisWrapper
from tests.conftest import KAFKA_TOPIC_CONSUMER_FOR_UPLOADING_DATA, producer, create_processing, wait_for

async def check_user_in_db(db_session, user_id)-> bool:
    """Вернёт результат проверки на не наличие в БД User по его 'user_id'"""
    result = await db_session.execute(select(User).where(User.user_id == user_id))
    return result.scalar_one_or_none() is not None

async def check_resume_in_db_and_redis(db_session, resume_id: int)-> bool:
    """Вернёт результат проверки на не наличие в БД Resume по его 'resume_id'"""
    result = await db_session.execute(
        select(Resume).where(Resume.resume_id == resume_id)
    )
    return result.scalar_one_or_none()

async def check_requirements_in_db(db_session, requirements_id: int)-> bool:
    """Вернёт результат проверки на не наличие в БД Requirements по его 'requirements_id'"""
    result = await db_session.execute(
        select(Requirements).where(Requirements.requirements_id == requirements_id)
    )
    return result.scalar_one_or_none()

async def check_processing_in_db(db_session, processing_id: int) -> bool:
    """Вернёт результат проверки на наличие в БД Processing по его 'processing_id'"""
    result = await db_session.execute(
        select(Processing).where(Processing.processing_id == processing_id)
    )
    return result.scalar_one_or_none()

async def check_delete_processing_in_db(db_session, processing_id: int) -> bool:
    """Вернёт результат проверки на не наличие в БД Processing по его 'processing_id'"""
    result = await db_session.execute(
        select(Processing).where(Processing.processing_id == processing_id)
    )
    if not result.scalar_one_or_none():
        return True
    else:
        return False


@pytest.mark.asyncio
async def test_handler_key_new_user(clearing_kafka, db_session):
    data_for_kafka = {
        'user_id': 1,
        'username': 'test_username',
        'full_name': 'test_full_name',
        'created_at': '2025-07-26 11:25:37.981839+00:00'
    }
    producer.sent_message(
        KAFKA_TOPIC_CONSUMER_FOR_UPLOADING_DATA,
        KEY_NEW_USER,
        data_for_kafka
    )

    # ожидание обработки
    await wait_for(lambda: check_user_in_db(db_session, data_for_kafka['user_id']), timeout=30)

    request_db = await db_session.execute(select(User).where(User.user_id == data_for_kafka['user_id']))
    data_db = request_db.scalar_one_or_none()

    assert data_db
    assert data_db.username == data_for_kafka['username']
    assert data_db.full_name == data_for_kafka['full_name']
    assert str(data_db.created_at) == data_for_kafka['created_at']

@pytest.mark.asyncio
async def test_handler_key_new_resume(clearing_kafka, db_session, create_user):
    print("вошли в функцию")
    data_for_kafka = {
        'resume_id': 1,
        'user_id': create_user['user_id'],
        'resume': 'test_resume'
    }
    producer.sent_message(
        KAFKA_TOPIC_CONSUMER_FOR_UPLOADING_DATA,
        KEY_NEW_RESUME,
        data_for_kafka
    )

    # ожидание обработки
    await wait_for(lambda: check_resume_in_db_and_redis(db_session, data_for_kafka['resume_id']), timeout=30)

    request_db = await db_session.execute(select(Resume).where(Resume.resume_id == data_for_kafka['resume_id']))
    data_db = request_db.scalar_one_or_none()
    assert data_db

    async with RedisWrapper() as redis:
        resume_list_json = await redis.get(f'resume:{data_for_kafka['resume_id']}')
        assert resume_list_json
        data_redis = json.loads(resume_list_json)

    assert data_for_kafka['resume_id'] == data_db.resume_id == data_redis['resume_id']
    assert data_for_kafka['user_id'] == data_db.user_id == data_redis['user_id']
    assert data_for_kafka['resume'] == data_db.resume == data_redis['resume']

@pytest.mark.asyncio
async def test_handler_key_new_requirements(clearing_kafka, db_session, create_user):
    data_for_kafka = {
        'requirements_id': 1,
        'user_id': create_user['user_id'],
        'requirements': 'test_requirements'
    }
    producer.sent_message(
        KAFKA_TOPIC_CONSUMER_FOR_UPLOADING_DATA,
        KEY_NEW_REQUIREMENTS,
        data_for_kafka
    )

    # ожидание обработки
    await wait_for(lambda: check_requirements_in_db(db_session, data_for_kafka['requirements_id']), timeout=30)

    request_db = await db_session.execute(select(Requirements).where(Requirements.requirements_id == data_for_kafka['requirements_id']))
    data_db = request_db.scalar_one_or_none()
    assert data_db

    async with RedisWrapper() as redis:
        requirements_list_json = await redis.get(f'requirements:{data_for_kafka['user_id']}')
        assert requirements_list_json
        requirements_list = json.loads(requirements_list_json)

        for requirements in requirements_list:
            if requirements['requirements_id'] == data_for_kafka['requirements_id']: data_redis = requirements

    assert data_for_kafka['requirements_id'] == data_db.requirements_id == data_redis['requirements_id']
    assert data_for_kafka['user_id'] == data_db.user_id == data_redis['user_id']
    assert data_for_kafka['requirements'] == data_db.requirements == data_redis['requirements']

@pytest.mark.asyncio
async def test_handler_key_new_processing(clearing_kafka, db_session, create_resume_and_requirements):
    data_for_kafka = {
        'processing_id': 1,
        'user_id': create_resume_and_requirements['user_id'],
        'resume_id': create_resume_and_requirements['resume_id'],
        'requirements_id': create_resume_and_requirements['requirements_id'],
        'score': 60,
        'matches': ['kafka', 'sql', 'python'],
        'recommendation': 'test_recommendation',
        'verdict': 'test_verdict',
    }
    producer.sent_message(
        KAFKA_TOPIC_CONSUMER_FOR_UPLOADING_DATA,
        KEY_NEW_PROCESSING,
        data_for_kafka
    )

    # ожидание обработки
    await wait_for(lambda: check_processing_in_db(db_session, data_for_kafka['processing_id']), timeout=30)

    request_db = await db_session.execute(select(Processing).where(Processing.processing_id == data_for_kafka['processing_id']))
    data_db = request_db.scalar_one_or_none()
    assert data_db

    # ключи для redis
    main_redis_key = f"processing:{data_for_kafka['user_id']}"
    requirements_key = f"processing_requirements:{data_for_kafka['user_id']}:{data_for_kafka['requirements_id']}"

    async with RedisWrapper() as redis:
        # Поиск по ключу main_redis_key
        main_processing_list_json = await redis.get(main_redis_key)
        assert main_processing_list_json
        main_processing_list = json.loads(main_processing_list_json)

        for processing in main_processing_list:
            if processing['processing_id'] == data_for_kafka['processing_id']: data_redis_main = processing

        # Поиск по ключу requirements_key
        requirements_processing_list_json = await redis.get(requirements_key)
        assert requirements_processing_list_json
        requirements_processing_list = json.loads(requirements_processing_list_json)

        for processing in requirements_processing_list:
            if processing['processing_id'] == data_for_kafka['processing_id']: data_redis_requirements = processing


    assert data_for_kafka['processing_id'] == data_db.processing_id == data_redis_main['processing_id'] == data_redis_requirements ['processing_id']
    assert data_for_kafka['user_id'] == data_db.user_id == data_redis_main['user_id'] == data_redis_requirements ['user_id']
    assert data_for_kafka['resume_id'] == data_db.resume_id == data_redis_main['resume_id'] == data_redis_requirements ['resume_id']
    assert data_for_kafka['requirements_id'] == data_db.requirements_id == data_redis_main['requirements_id'] == data_redis_requirements ['requirements_id']
    assert data_for_kafka['score'] == data_db.score == data_redis_main['score'] == data_redis_requirements ['score']
    assert data_for_kafka['matches'] == data_db.matches == data_redis_main['matches'] == data_redis_requirements ['matches']
    assert data_for_kafka['recommendation'] == data_db.recommendation == data_redis_main['recommendation'] == data_redis_requirements ['recommendation']
    assert data_for_kafka['verdict'] == data_db.verdict == data_redis_main['verdict'] == data_redis_requirements ['verdict']

class TestDeletions:
    async def test_handler_key_delete_processing(self, db_session, clearing_kafka, create_resume_and_requirements):
        processing = await create_processing(db_session, create_resume_and_requirements['user_id'], create_resume_and_requirements['requirements_id'])

        data_processing = {
            'processing_id': processing['processing_id'],
            'user_id': create_resume_and_requirements['user_id'],
            'resume_id': create_resume_and_requirements['resume_id'],
            'requirements_id': create_resume_and_requirements['requirements_id'],
            'score': 60,
            'matches': ['kafka', 'sql', 'python'],
            'recommendation': 'test_recommendation',
            'verdict': 'test_verdict',
        }

        main_redis_key = f"processing:{create_resume_and_requirements['user_id']}"
        requirements_key = f"processing_requirements:{create_resume_and_requirements['user_id']}:{create_resume_and_requirements['requirements_id']}"

        async with RedisWrapper() as redis:
            await redis.set(main_redis_key, json.dumps([data_processing]))
            await redis.set(requirements_key, json.dumps([data_processing]))

        data_for_kafka = {
            'processing_ids': [processing['processing_id']],
            'user_id': create_resume_and_requirements['user_id']
        }
        producer.sent_message(
            KAFKA_TOPIC_CONSUMER_FOR_UPLOADING_DATA,
            KEY_DELETE_PROCESSING,
            data_for_kafka
        )

        # ожидание обработки
        await wait_for(lambda: check_delete_processing_in_db(db_session, processing['processing_id']), timeout=30)

        result_db = await db_session.execute(select(Processing).where(Processing.processing_id == processing['processing_id']))
        processing_result = result_db.scalar_one_or_none()
        assert not processing_result

        async with RedisWrapper() as redis:
            assert not json.loads(await redis.get(main_redis_key))
            assert not json.loads(await redis.get(requirements_key))

    async def test_handler_key_delete_requirements(self, db_session, clearing_kafka, create_resume_and_requirements):
        processing = await create_processing(db_session, create_resume_and_requirements['user_id'], create_resume_and_requirements['requirements_id'])

        data_processing = {
            'processing_id': processing['processing_id'],
            'user_id': create_resume_and_requirements['user_id'],
            'resume_id': create_resume_and_requirements['resume_id'],
            'requirements_id': create_resume_and_requirements['requirements_id'],
            'score': 60,
            'matches': ['kafka', 'sql', 'python'],
            'recommendation': 'test_recommendation',
            'verdict': 'test_verdict',
        }

        main_redis_key = f"processing:{create_resume_and_requirements['user_id']}"
        requirements_key = f"processing_requirements:{create_resume_and_requirements['user_id']}:{create_resume_and_requirements['requirements_id']}"

        async with RedisWrapper() as redis:
            await redis.set(main_redis_key, json.dumps([data_processing]))
            await redis.set(requirements_key, json.dumps([data_processing]))

        data_for_kafka = {
            'processing_ids': [processing['processing_id']],
            'requirements_ids': [create_resume_and_requirements['requirements_id']],
            'user_id': create_resume_and_requirements['user_id']
        }
        producer.sent_message(
            KAFKA_TOPIC_CONSUMER_FOR_UPLOADING_DATA,
            KEY_DELETE_REQUIREMENTS,
            data_for_kafka
        )

        # ожидание обработки
        await wait_for(lambda: check_delete_processing_in_db(db_session, processing['processing_id']), timeout=30)

        result_db = await db_session.execute(select(Processing).where(Processing.processing_id == processing['processing_id']))
        processing_result = result_db.scalar_one_or_none()
        assert not processing_result

        result_db = await db_session.execute(select(Requirements).where(Requirements.requirements_id == create_resume_and_requirements['requirements_id']))
        requirements_result = result_db.scalar_one_or_none()
        assert not requirements_result

        async with RedisWrapper() as redis:
            assert not json.loads(await redis.get(main_redis_key))
            assert not json.loads(await redis.get(requirements_key))