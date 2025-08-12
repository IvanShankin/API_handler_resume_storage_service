import json
import os

from typing import List, Annotated, Optional, Union
from enum import Enum
from fastapi import Path, Query, APIRouter, Depends, HTTPException, Form, Request
from sqlalchemy import text, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from srt.access import get_current_user
from srt.config import logger, STORAGE_TIME_DATA, STORAGE_TIME_ALLS_DATA
from srt.database.database import get_db
from srt.database.models import User, Resume, Requirements, Processing
from srt.dependencies.redis_dependencies import Redis, get_redis
from srt.exception import NoRights, NotFoundData, InvalidParameters
from srt.schemas.request import ProcessingAndRequirementsID, SortField, SortOrder
from srt.schemas.response import UserOut, ResumeOut, RequirementsOut, ProcessingOut, ProcessingDetailOut

router = APIRouter()

async def chek_rights_redis(key: str, user_id: int, redis: Redis):
    """
    проверяет в redis по ключу, если user_id не совпадает, то вернёт ошибку NoRights
    :return: Значение redis по данному ключу
    """
    data_redis = await redis.get(key)
    if data_redis:
        data_redis = json.loads(data_redis)
        if int(data_redis['user_id']) != user_id:
            raise NoRights()
    return data_redis


async def validate_processing_data(
    processing_id: Optional[int] = Query(None, ge=1, description="Фильтр по ID обработки."),
    requirements_id: Optional[int] = Query(None, ge=1, description="Фильтр по ID требования.")
)->ProcessingAndRequirementsID:
    """Проверяет данные направленные для получения обработки. Вызовет ошибку если будут переданы сразу два параметра"""
    if processing_id and requirements_id:
        raise InvalidParameters(['processing_id', 'requirements_id'])

    return ProcessingAndRequirementsID(
        processing_id = processing_id,
        requirements_id = requirements_id,
    )

# вспомогательные функции для search_processing
async def prepare_processing_data(p: Processing) -> dict:
    """Подготавливает данные для кеширования"""
    return {
        "processing_id": p.processing_id,
        "resume_id": p.resume_id,
        "requirements_id": p.requirements_id,
        "user_id": p.user_id,
        "create_at": p.create_at.strftime("%Y-%m-%d %H:%M:%S%z"),
        "score": p.score,
        "matches": p.matches,
        "recommendation": p.recommendation,
        "verdict": p.verdict,
        "resume": p.resume.resume if p.resume else None,
        "requirements": p.requirements.requirements if p.requirements else None
    }

def _convert_to_output_model(data: str, in_detail: bool)->Union[List[ProcessingOut], List[ProcessingDetailOut]]:
    """
    Конвертирует в выходную модель. Если необходима полная информация, то преобразует в ProcessingDetailOut, иначе в ProcessingOut
    :param data: json строка в формате list[dict]
    :param in_detail: флаг необходимости получения подробностей, если указать, то вернётся ProcessingDetailOut
    """
    data: list[dict] = json.loads(data)
    if in_detail:
        # словарь конвертируем в ProcessingDetailOut
        return [ProcessingDetailOut.model_validate(item) for item in data]
    # создаём новый словарь, устанавливая некоторые данные на None (**item - распаковывает словарь)
    return [ProcessingOut.model_validate({**item, "resume": None, "requirements": None}) for item in data]

async def search_processing(
        processing_id: Optional[int],
        requirements_id: Optional[int],
        user_id: int,
        in_detail: bool,
        redis: Redis,
        db: AsyncSession
    )->Union[List[ProcessingOut], List[ProcessingDetailOut]]:
    """
    Ищет данные об обработке в redis, если там не найдено, то ищет в БД. Кэширует данные в redis.
    :return: Ввернёт List[ProcessingDetailOut], если установлен флаг in_detail, иначе List[ProcessingOut]
    """

    # Основной ключ для всех processing пользователя. Хранит в виде: [{...}, {...}]
    main_redis_key = f"processing:{user_id}"

    # Дополнительный ключ для частых запросов (кеш второго уровня).
    # Хранит только обработки с данным requirements_id в виде: [{...}, {...}]
    requirements_key = f"processing_requirements:{user_id}:{requirements_id}" if requirements_id else None

    # проверка специализированных ключей
    if requirements_key: # если необходимо изъять данные только с указанным requirements_id
        data_redis = await redis.get(requirements_key)
        if data_redis:
            await redis.setex(
                requirements_key,
                STORAGE_TIME_DATA,
                data_redis
            )  # обновляем срок кэширования
            return _convert_to_output_model(data_redis, in_detail)

    # проверка основного кеша
    data_redis = await redis.get(main_redis_key)
    if data_redis:
        processing = json.loads(data_redis) # преобразование с json в обычные данные (тут будет: [{...}, {...}])

        # фильтрация данных с redis
        if processing_id: # если необходимо вернуть только одно значение по переданному processing_id
            for p in processing:
                if p["processing_id"] == processing_id:
                    processing = [p]
                    break
        elif requirements_id: # если необходимо вернуть значения только с requirements_id
            processing = [p for p in processing if p["requirements_id"] == requirements_id]

        if processing:
            await redis.setex(
                main_redis_key,
                STORAGE_TIME_ALLS_DATA,
                json.dumps(processing)
            ) # обновляем срок кэширования
            return _convert_to_output_model(json.dumps(processing), in_detail)

    # запрос к БД
    query = (select(Processing).where(Processing.user_id == user_id)
             .options(selectinload(Processing.resume), selectinload(Processing.requirements)))

    if processing_id:
        query = query.where(Processing.processing_id == processing_id)
    elif requirements_id:
        query = query.where(Processing.requirements_id == requirements_id)

    result = await db.execute(query)
    db_processing = result.scalars().all()

    if not db_processing:
        raise NotFoundData()

    # подготовка и кэширование данных
    processed_data = [await prepare_processing_data(p) for p in db_processing]

    # основной кеш (все данные пользователя)
    await redis.setex(
        main_redis_key,
        STORAGE_TIME_ALLS_DATA,
        json.dumps(processed_data)
    )

    # кеш для частых запросов (по требованию)
    if requirements_id:
        req_processing = [p for p in processed_data if p["requirements_id"] == requirements_id]
        if req_processing:
            await redis.setex(
                requirements_key,
                STORAGE_TIME_DATA,
                json.dumps(req_processing)
            )

    # возврат результата
    data_to_return = processed_data
    if processing_id:
        data_to_return = [p for p in processed_data if p["processing_id"] == processing_id]
    elif requirements_id:
        data_to_return = [p for p in processed_data if p["requirements_id"] == requirements_id]

    return _convert_to_output_model(json.dumps(data_to_return), in_detail)

async def sorted_list_processing(
        processings: List[ProcessingOut],
        sort_by: SortField = SortField.CREATE_AT,
        order: SortOrder = SortOrder.DESC,
)->List[ProcessingOut]:
    """
    Сортирует список обработок по указанному полю и порядку

    :param processings: Список обработок для сортировки
    :param sort_by: Поле для сортировки (create_at или score)
    :param order: Порядок сортировки (asc или desc)
    :return: Отсортированный список ProcessingOut
    """
    reverse = order == SortOrder.DESC

    if sort_by == SortField.CREATE_AT:
        return sorted(
            processings,
            key=lambda x: x.create_at, # отсортирует каждый элемент списка по переменной create_at
            reverse=reverse
        )
    elif sort_by == SortField.SCORE:
        return sorted(
            processings,
            key=lambda x: x.score,
            reverse=reverse
        )
    return processings

@router.get("/health")
async def health_check(
        db: AsyncSession = Depends(get_db),
        redis_client: Redis = Depends(get_redis)
):
    # Проверка БД
    try:
        await db.execute(text("SELECT 1"))
    except Exception:
        logger.error("Database connection failed")
        raise HTTPException(500, "Database unavailable")

    # Проверка Redis
    try:
        await redis_client.ping()
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")
        raise HTTPException(500, "Redis unavailable")

    return {"status": "OK"}


@router.get("/me", response_model=UserOut)
async def get_me(
        current_user: User = Depends(get_current_user)
):
    return UserOut(
        user_id = current_user.user_id,
        username = current_user.username,
        full_name = current_user.full_name,
        created_at = current_user.created_at,
    )


@router.get("/get_resume", response_model=ResumeOut)
async def get_resume(
        resume_id: int,
        current_user: User = Depends(get_current_user),
        redis: Redis = Depends(get_redis),
        db: AsyncSession = Depends(get_db)
):
    redis_data = await chek_rights_redis(f'resume:{resume_id}', current_user.user_id, redis)
    if redis_data:
        await redis.setex(f'resume:{resume_id}', STORAGE_TIME_DATA, json.dumps(redis_data))
        return ResumeOut(resume_id=redis_data['resume_id'], user_id=redis_data['user_id'], resume=redis_data['resume'])

    db_data = await db.execute(select(Resume).where(Resume.resume_id == resume_id))
    db_resume = db_data.scalar_one_or_none()
    if db_resume: # если данные с БД есть
        if db_resume.user_id == current_user.user_id:
            await redis.setex(
                f'resume:{resume_id}',
                STORAGE_TIME_DATA,
                json.dumps({'resume_id': db_resume.resume_id, 'user_id': db_resume.user_id, 'resume': db_resume.resume}))
            return ResumeOut(resume_id=db_resume.resume_id, user_id=db_resume.user_id, resume=db_resume.resume)
        else:
            raise NoRights()
    else:
        raise NotFoundData()

@router.get("/get_requirements", response_model=List[RequirementsOut])
async def get_requirements(
        requirements_id: Optional[int] = Query(None, ge=1, description="Указание конкретного требования (Опционально)"),
        current_user: User = Depends(get_current_user),
        redis: Redis = Depends(get_redis),
        db: AsyncSession = Depends(get_db)
):
    """Если не указать requirements_id, то вернутся все требования данного пользователя"""
    redis_key = f'requirements:{current_user.user_id}'

    try:
        redis_data = await chek_rights_redis(redis_key, current_user.user_id, redis)
        if redis_data:
            await redis.setex(redis_key, STORAGE_TIME_DATA, json.dumps(redis_data)) # продлеваем время хранения

            if requirements_id: # если необходимо вернуть только одно значение
                for req  in redis_data:
                    if req ['requirements_id'] == requirements_id:
                        return [RequirementsOut.model_validate(req)]
                raise NotFoundData() # Необходимо вернуть ошибку, т.к. В redis хранятся все данные данного пользователя
            else:
                return [RequirementsOut.model_validate(req) for req in redis_data]
    except Exception as e:
        logger.error(f"Redis error: {e}")


    # Запрос к БД
    query = select(Requirements).where(Requirements.user_id == current_user.user_id)
    if requirements_id:
        query = query.where(Requirements.requirements_id == requirements_id)

    result = await db.execute(query)
    db_requirements = result.scalars().all()

    if not db_requirements:
        raise NotFoundData()

    # Подготовка данных для кэша и ответа
    requirements_data = [req.to_dict() for req in db_requirements]
    await redis.setex(
        redis_key,
        STORAGE_TIME_DATA,
        json.dumps(requirements_data)
    )

    return [RequirementsOut.model_validate(req) for req in requirements_data]


@router.get('/get_processing', response_model=List[ProcessingOut])
async def get_processing(
        param: ProcessingAndRequirementsID = Depends(validate_processing_data),
        sort_by: SortField = Query(SortField.CREATE_AT, description="Поле для сортировки"),
        order: SortOrder = Query(SortOrder.DESC, description="Порядок сортировки"),
        current_user: User = Depends(get_current_user),
        redis: Redis = Depends(get_redis),
        db: AsyncSession = Depends(get_db)
):
    """
    processing_id и requirements_id нельзя отправлять в одном запросе!
    :return: вернёт обработку без указания конкретных данных о резюме и требованиях, только их id
    """
    processing = await search_processing(param.processing_id, param.requirements_id, current_user.user_id, False, redis, db)

    return await sorted_list_processing(processing, sort_by, order)

@router.get('/get_processing_detail', response_model=List[ProcessingDetailOut])
async def get_processing_detail(
        param: ProcessingAndRequirementsID = Depends(validate_processing_data),
        sort_by: SortField = Query(SortField.CREATE_AT, description="Поле для сортировки"),
        order: SortOrder = Query(SortOrder.DESC, description="Порядок сортировки"),
        current_user: User = Depends(get_current_user),
        redis: Redis = Depends(get_redis),
        db: AsyncSession = Depends(get_db)
):
    """
    processing_id и requirements_id нельзя отправлять в одном запросе!
    :return: вернёт обработку с полными данными о резюме и требованиях к нему
    """
    processing = await search_processing(param.processing_id, param.requirements_id, current_user.user_id, True, redis, db)

    return await sorted_list_processing(processing, sort_by, order)
