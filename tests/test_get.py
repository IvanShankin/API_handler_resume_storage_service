import json
from datetime import datetime

import pytest
from httpx import AsyncClient, ASGITransport

from srt.dependencies.redis_dependencies import RedisWrapper
from srt.main import app
from tests.conftest import DICT_FOR_PROCESSING, DICT_FOR_PROCESSING_DETAIL, create_processing


@pytest.mark.asyncio
async def test_health_check():
    async with AsyncClient(
            transport=ASGITransport(app),
            base_url="http://test",
    ) as ac:
        response = await ac.get("/health")
        assert response.status_code == 200

@pytest.mark.asyncio
async def test_me(create_user):
    async with AsyncClient(
            transport=ASGITransport(app),
            base_url="http://test",
    ) as ac:
        response = await ac.get("/me", headers={"Authorization": f"Bearer {create_user['access_token']}"})
        assert response.status_code == 200

        data_response = response.json()

        assert data_response['user_id'] == create_user['user_id']
        assert data_response['username'] == create_user['username']
        assert data_response['full_name'] == create_user['full_name']
        assert str(data_response['created_at']).replace('T', ' ').replace('Z', '+00:00') == str(create_user['created_at'])


@pytest.mark.parametrize(
    'use_redis',
    [True,False,]
)
@pytest.mark.asyncio
async def test_get_resume(use_redis, create_resume):
    redis_key = f'resume:{create_resume['user_id']}'

    if use_redis:
        async with RedisWrapper() as redis:
            await redis.set(
                redis_key,
                json.dumps({
                    'resume_id': create_resume['resume_id'],
                    'user_id': create_resume['user_id'],
                    'resume': create_resume['resume']
                })
            )

    async with AsyncClient(
            transport=ASGITransport(app),
            base_url="http://test",
    ) as ac:
        response = await ac.get(
            "/get_resume",
            params={'resume_id': create_resume['resume_id']},
            headers={"Authorization": f"Bearer {create_resume['access_token']}"}
        )
        assert response.status_code == 200

        data_response = response.json()

        async with RedisWrapper() as redis:
            data_redis = json.loads(await redis.get(redis_key))
            assert data_redis

        assert data_response['resume_id'] == create_resume['resume_id'] == data_redis['resume_id']
        assert data_response['user_id'] == create_resume['user_id'] == data_redis['user_id']
        assert data_response['resume'] == create_resume['resume'] == data_redis['resume']


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'use_redis',
    [True,False,]
)
async def test_get_requirements(use_redis, create_requirements):
    redis_key = f'requirements:{create_requirements['user_id']}'

    if use_redis:
        async with RedisWrapper() as redis:
            await redis.set(
                redis_key,
                json.dumps([{
                    'requirements_id': create_requirements['requirements_id'],
                    'user_id': create_requirements['user_id'],
                    'requirements': create_requirements['requirements']
                }])
            )

    async with AsyncClient(
            transport=ASGITransport(app),
            base_url="http://test",
    ) as ac:
        response = await ac.get(
            "/get_requirements",
            params={'requirements_id': create_requirements['requirements_id']},
            headers={"Authorization": f"Bearer {create_requirements['access_token']}"}
        )
        assert response.status_code == 200

        data_response = response.json()[0] # берём первый элемент списка

        async with RedisWrapper() as redis:
            requirements_list = json.loads(await redis.get(redis_key))
            for requirements in requirements_list:
                if requirements['requirements_id'] == create_requirements['requirements_id']: data_redis = requirements

            assert data_redis

        assert data_response['requirements_id'] == create_requirements['requirements_id'] == data_redis['requirements_id']
        assert data_response['user_id'] == create_requirements['user_id'] == data_redis['user_id']
        assert data_response['requirements'] == create_requirements['requirements'] == data_redis['requirements']


class TestGetProcessing:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'use_redis, url_request, expected_api_fields',
        [
            # проверка get_processing
            (True, '/get_processing', DICT_FOR_PROCESSING),
            (False, '/get_processing', DICT_FOR_PROCESSING),

            # проверка get_processing_detail
            (True, '/get_processing_detail', DICT_FOR_PROCESSING_DETAIL),
            (False, '/get_processing_detail', DICT_FOR_PROCESSING_DETAIL),
        ]
    )
    async def test_get_all(self, use_redis, url_request, expected_api_fields, db_session,  create_user):
        main_redis_key = f'processing:{create_user['user_id']}'

        processing_list = []
        length_processing_list = 5

        for i in range(length_processing_list):
            processing_list.append(await create_processing(db_session, create_user['user_id'], i))

        if use_redis:
            async with RedisWrapper() as redis:
                await redis.set(main_redis_key,json.dumps(processing_list) )

        async with AsyncClient(
                transport=ASGITransport(app),
                base_url="http://test",
        ) as ac:
            response = await ac.get(
                url_request,
                headers={"Authorization": f"Bearer {create_user['access_token']}"}
            ) # пробуем взять всё
            assert response.status_code == 200

            data_response = response.json()
            # проверяем условия на количество вернувшихся объектов
            assert len(data_response) == length_processing_list

            # Проверяем данные из API
            for api_item in data_response:
                # Находим соответствующий исходный объект
                original_item = next(
                    p for p in processing_list
                    if p['processing_id'] == api_item['processing_id']
                )

                # Проверяем, что API вернуло только ожидаемые поля
                assert set(api_item.keys()) == expected_api_fields

                # Проверяем значения полей
                for field in expected_api_fields:
                    assert api_item[field] == original_item[field]

            # проверяем данные в Redis
            async with RedisWrapper() as redis:
                redis_data = json.loads(await redis.get(main_redis_key))
                assert len(redis_data) == length_processing_list

                # Проверяем, что в Redis сохранились полные данные
                for redis_item in redis_data:
                    original_item = next(
                        p for p in processing_list
                        if p['processing_id'] == redis_item['processing_id']
                    )
                    # Проверяем, что все поля исходных данных есть в Redis
                    for field in original_item:
                        if field == 'create_at':
                            assert datetime.fromisoformat(redis_item[field]) == datetime.fromisoformat(original_item[field])
                        else:
                            assert redis_item[field] == original_item[field]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'use_redis, url_request, expected_api_fields',
        [
            # проверка get_processing
            (True, '/get_processing', DICT_FOR_PROCESSING),
            (False, '/get_processing', DICT_FOR_PROCESSING),

            # проверка get_processing_detail
            (True, '/get_processing_detail', DICT_FOR_PROCESSING_DETAIL),
            (False, '/get_processing_detail', DICT_FOR_PROCESSING_DETAIL),
        ]
    )
    async def test_get_by_processing_id(self, use_redis, url_request, expected_api_fields, db_session, create_user):
        main_redis_key = f'processing:{create_user['user_id']}'

        original_data = await create_processing(db_session, create_user['user_id'], 1)
        false_data = await create_processing(db_session, create_user['user_id'], 2) # создание данных с другим processing_id

        if use_redis:
            async with RedisWrapper() as redis:
                await redis.set(main_redis_key,json.dumps([original_data]))
                await redis.set(main_redis_key,json.dumps([false_data]))

        async with AsyncClient(
                transport=ASGITransport(app),
                base_url="http://test",
        ) as ac:
            response = await ac.get(
                url_request,
                params={'processing_id': original_data['processing_id']},
                headers={"Authorization": f"Bearer {create_user['access_token']}"}
            ) # пробуем взять всё
            assert response.status_code == 200

            data_response = response.json()[0] # берём первый элемент списка

            # проверяем, что API вернуло только ожидаемые поля
            assert set(data_response.keys()) == expected_api_fields

            # проверяем значения полей
            for field in expected_api_fields:
                assert data_response[field] == original_data[field]

            # проверяем данные в Redis
            async with RedisWrapper() as redis:
                redis_data = json.loads(await redis.get(main_redis_key))[0] # берём первый элемент

                # Проверяем, что все поля исходных данных есть в Redis
                for field in redis_data:
                    if field == 'create_at':
                        assert datetime.fromisoformat(redis_data[field]) == datetime.fromisoformat(original_data[field])
                    else:
                        assert redis_data[field] == original_data[field]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'use_redis, url_request, expected_api_fields',
        [
            # проверка get_processing
            (True, '/get_processing', DICT_FOR_PROCESSING),
            (False, '/get_processing', DICT_FOR_PROCESSING),

            # проверка get_processing_detail
            (True, '/get_processing_detail', DICT_FOR_PROCESSING_DETAIL),
            (False, '/get_processing_detail', DICT_FOR_PROCESSING_DETAIL),
        ]
    )
    async def test_get_by_processing_id(self, use_redis, url_request, expected_api_fields, db_session, create_user):
        length_processing_list = 5

        requirements_id = 1
        false_requirements_id = 2

        requirements_redis_key = f'processing_requirements:{create_user['user_id']}:{requirements_id}'
        false_requirements_redis_key = f'processing_requirements:{create_user['user_id']}:{false_requirements_id}'

        processing_list = []
        false_processing_list = []

        for i in range(length_processing_list):
            processing_list.append(await create_processing(db_session, create_user['user_id'], 1))
            false_processing_list.append(await create_processing(db_session, create_user['user_id'], false_requirements_id))

        if use_redis:
            async with RedisWrapper() as redis:
                await redis.set(requirements_redis_key, json.dumps(processing_list))
                await redis.set(false_requirements_redis_key, json.dumps(processing_list))

        async with AsyncClient(
                transport=ASGITransport(app),
                base_url="http://test",
        ) as ac:
            response = await ac.get(
                url_request,
                params={'requirements_id': requirements_id},
                headers={"Authorization": f"Bearer {create_user['access_token']}"}
            )  # пробуем взять всё
            assert response.status_code == 200

            data_response = response.json()

            print(data_response)
            print(processing_list)

            # Проверяем данные из API
            for api_item in data_response:
                # Находим соответствующий исходный объект
                original_item = next(
                    p for p in processing_list
                    if p['processing_id'] == api_item['processing_id']
                )

                # Проверяем, что API вернуло только ожидаемые поля
                assert set(api_item.keys()) == expected_api_fields

                # Проверяем значения полей
                for field in expected_api_fields:
                    assert api_item[field] == original_item[field]

            # проверяем данные в Redis
            async with RedisWrapper() as redis:
                redis_data = json.loads(await redis.get(requirements_redis_key))
                assert len(redis_data) == length_processing_list

                # Проверяем, что в Redis сохранились полные данные
                for redis_item in redis_data:
                    original_item = next(
                        p for p in processing_list
                        if p['processing_id'] == redis_item['processing_id']
                    )
                    # Проверяем, что все поля исходных данных есть в Redis
                    for field in original_item:
                        if field == 'create_at':
                            assert datetime.fromisoformat(redis_item[field]) == datetime.fromisoformat(
                                original_item[field])
                        else:
                            assert redis_item[field] == original_item[field]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'url_request, expected_api_fields, sort_field,  type_sort',
        [
            # проверка get_processing
            ('/get_processing', DICT_FOR_PROCESSING, 'create_at', 'asc'),
            ('/get_processing', DICT_FOR_PROCESSING, 'create_at', 'desc'),
            ('/get_processing', DICT_FOR_PROCESSING, 'score', 'asc'),
            ('/get_processing', DICT_FOR_PROCESSING, 'score', 'desc'),

            # проверка get_processing_detail
            ('/get_processing_detail', DICT_FOR_PROCESSING_DETAIL, 'create_at', 'asc'),
            ('/get_processing_detail', DICT_FOR_PROCESSING_DETAIL, 'create_at', 'desc'),
            ('/get_processing_detail', DICT_FOR_PROCESSING_DETAIL, 'score', 'asc'),
            ('/get_processing_detail', DICT_FOR_PROCESSING_DETAIL, 'score', 'desc'),
        ]
    )
    async def test_get_sorted(self, url_request, expected_api_fields, sort_field, type_sort, db_session, create_user):
        main_redis_key = f'processing:{create_user['user_id']}'

        processing_list = []
        length_processing_list = 5

        for i in range(length_processing_list):
            processing_list.append(await create_processing(db_session, create_user['user_id'], i))

        # сортируем список
        if type_sort == 'asc':
            processing_list = sorted(processing_list, key=lambda x: x[sort_field], reverse=False)
        else:
            processing_list = sorted(processing_list, key=lambda x: x[sort_field], reverse=True)


        async with RedisWrapper() as redis:
            await redis.set(main_redis_key, json.dumps(processing_list))

        async with AsyncClient(
                transport=ASGITransport(app),
                base_url="http://test",
        ) as ac:
            response = await ac.get(
                url_request,
                params={'sort_by': sort_field, 'order': type_sort},
                headers={"Authorization": f"Bearer {create_user['access_token']}"}
            )  # пробуем взять всё
            assert response.status_code == 200

            data_response = response.json()
            # проверяем условия на количество вернувшихся объектов
            assert len(data_response) == length_processing_list

            for i in range(length_processing_list):
                for field in expected_api_fields:
                    assert data_response[i][field] == processing_list[i][field], f"поле '{field}' не совпало"

