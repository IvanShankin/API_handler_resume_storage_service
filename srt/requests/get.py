import os
from typing import List, Annotated, Optional

from fastapi import Path, Query
from sqlalchemy import text, select
from sqlalchemy.ext.asyncio import AsyncSession

from fastapi import APIRouter, Depends, HTTPException, Form, Request

from srt.access import get_current_user
from srt.config import logger, MAX_STORAGE_TIME_DATA
from srt.database.database import get_db
from srt.database.models import User, Resume, Requirements, Processing
from srt.dependencies.redis_dependencies import Redis, get_redis
from srt.exception import InvalidCredentialsException, NoRights, NotFoundData
from srt.schemas.response import UserOut, ResumeOut, RequirementsOut, ProcessingOut, ProcessingDetailOut

router = APIRouter()


async def chek_rights_redis(key: str, user_id: int, redis: Redis):
    """
    проверяет в redis по ключу, если user_id не совпадает, то вернёт ошибку NoRights
    :returns Значение redis по данному ключу
    """
    data_redis = await redis.get(key)
    if data_redis['user_id'] != user_id:
        raise NoRights()
    return data_redis

async def search_processing(processing_id: int, user_id: int, redis: Redis, db: AsyncSession)->List[Processing]:
    """
    Ищет processing сначала в Redis, потом в БД.
    Если не найдено - вызывает NotFoundData().
    Если user_id не совпадает - вызывает NoRights().
    """
    if processing_id:
        # поиск в redis
        redis_data = await chek_rights_redis(f"processing:{processing_id}", user_id, redis)
        if redis_data:
            cached_processing = ProcessingOut.model_validate_json(redis_data)  # Конвертируем в ProcessingOut
            processing = [Processing(**cached_processing.model_dump())]  # Конвертируем в SQLAlchemy модель
        else:
            # поиск в БД
            result_db = await db.execute(
                select(Processing)
                .where(Processing.processing_id == processing_id)
            )
            processing = result_db.scalar_one_or_none()
            if not processing:
                raise NotFoundData()
            if processing.user_id != user_id:
                raise NoRights()

            processing = [processing]
    else:
        result_db = await db.execute(
            select(Processing)
            .where(Processing.user_id == user_id)
        )
        processing = result_db.scalars().all()
        if not processing:
            raise NotFoundData()

    return processing

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
        return ResumeOut(resume_id=resume_id, user_id=redis_data['user_id'], resume=redis_data['resume'])

    db_data = await db.execute(select(Resume).where(Resume.resume_id == resume_id))
    db_resume = db_data.scalar_one_or_none()
    if db_resume: # если данные с БД есть
        if db_resume.user_id == current_user.user_id:
            return ResumeOut(resume_id=db_resume.resume_id, user_id=db_resume.user_id, resume=db_resume.resume)
        else:
            raise NoRights()
    else:
        raise NotFoundData()

@router.get("/get_requirements", response_model=RequirementsOut)
async def get_requirements(
        requirements_id: int,
        current_user: User = Depends(get_current_user),
        redis: Redis = Depends(get_redis),
        db: AsyncSession = Depends(get_db)
):
    redis_data = await chek_rights_redis(f'requirements:{requirements_id}', current_user.user_id, redis)
    if redis_data:
        return RequirementsOut(requirements_id=requirements_id, user_id=redis_data['user_id'], requirements=redis_data['requirements'])

    db_data = await db.execute(select(Requirements).where(Requirements.requirements_id == requirements_id))
    db_requirements = db_data.scalar_one_or_none()
    if db_requirements: # если данные с БД есть
        if db_requirements.user_id == current_user.user_id:
            return RequirementsOut(requirements_id=db_requirements.requirements_id, user_id=db_requirements.user_id, requirements=db_requirements.requirements)
        else:
            raise NoRights()
    else:
        raise NotFoundData()


@router.get('/get_processing', response_model=List[ProcessingOut])
async def get_processing(
        processing_id: Optional[int]= Query(default=None, ge=1, description="Фильтр по ID обработки. Если не указан, возвращаются все обработки пользователя"),
        current_user: User = Depends(get_current_user),
        redis: Redis = Depends(get_redis),
        db: AsyncSession = Depends(get_db)
):
    """
    :return: вернёт обработку без указания конкретных данных о резюме и требованиях, только их id
    """
    processing = await search_processing(processing_id, current_user.user_id, redis, db)

    processing_out = None
    processing_list = []
    for p in processing:
        processing_out = ProcessingOut(
            processing_id=p.processing_id,
            resume_id=p.resume_id,
            requirements_id=p.requirements_id,
            user_id=p.user_id,
            score=p.score,
            matches=p.matches,
            recommendation=p.recommendation,
            verdict=p.verdict
        )
        processing_list.append(processing_out)

    if processing_id:  # записываем в redis только если явно указали processing_id
        await redis.setex(
            f"processing:{processing_out.processing_id}",
            MAX_STORAGE_TIME_DATA,
            processing_out.model_dump_json()
        )
    return processing_list

@router.get('/get_processing_detail', response_model=List[ProcessingDetailOut])
async def get_processing_detail(
        processing_id: Optional[int]= Query(default=None, ge=1, description="Фильтр по ID обработки. Если не указан, возвращаются все обработки пользователя"),
        current_user: User = Depends(get_current_user),
        redis: Redis = Depends(get_redis),
        db: AsyncSession = Depends(get_db)
):
    """
    :return: вернёт обработку с полными данными о резюме и требованиях к нему
    """
    processing = await search_processing(processing_id, current_user.user_id, redis, db)

    processing_out = None
    processing_list = []
    for p in processing:
        processing_out = ProcessingDetailOut(
            processing_id=p.processing_id,
            resume_id=p.resume_id,
            requirements_id=p.requirements_id,
            user_id=p.user_id,
            score=p.score,
            matches=p.matches,
            recommendation=p.recommendation,
            verdict=p.verdict,
            resume=p.resume.resume,
            requirements=p.requirements.requirements
        )
        processing_list.append(processing_out)

    if processing_id:  # записываем в redis только если явно указали processing_id
        await redis.setex(
            f"processing:{processing_out.processing_id}",
            MAX_STORAGE_TIME_DATA,
            processing_out.model_dump_json()
        )

    return processing_list
