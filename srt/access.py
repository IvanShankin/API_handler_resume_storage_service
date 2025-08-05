import os
from datetime import datetime, timedelta, UTC

import redis
from fastapi import Depends
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import ValidationError

from srt.database.database import get_db
from srt.database.models import User
from srt.dependencies.redis_dependencies import get_redis
from srt.exception import InvalidCredentialsException
from srt.schemas.response import UserOut

load_dotenv()
SECRET_KEY = os.getenv('SECRET_KEY')
ACCESS_TOKEN_EXPIRE_MINUTES = float(os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES'))
ALGORITHM = os.getenv('ALGORITHM')
oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="auth/login",
    scheme_name="OAuth2PasswordBearer",
    scopes={"read": "Read access", "write": "Write access"}
)


async def get_current_user(
        token: str = Depends(oauth2_scheme),
        db: AsyncSession = Depends(get_db),
        redis_client: redis.Redis = Depends(get_redis)
):
    try:
        # Декодируем токен
        payload = jwt.decode(
            token,
            SECRET_KEY,
            algorithms=[ALGORITHM],
            options={"verify_exp": True}
        )

        # Извлекаем ID пользователя
        user_id: str = payload.get("sub")
        if user_id is None:
            raise InvalidCredentialsException

        try:
            cached_user = await redis_client.get(f"user:{user_id}") # пытаемся найти в Redis
            if cached_user:
                return UserOut.model_validate_json(cached_user) # Pydantic парсит JSON
        except ValidationError:
            # Удаляем битый кэш и продолжаем
            await redis_client.delete(f"user:{user_id}")
    except JWTError:  # Ловим все ошибки JWT
        raise InvalidCredentialsException

    # проверяем существование пользователя
    result = await db.execute(select(User).where(User.user_id == int(user_id)))
    user = result.scalar_one_or_none()
    if user is None:
        raise InvalidCredentialsException

    # Конвертируем SQLAlchemy объект в словарь
    user_dict = {"user_id": user.user_id, 'username': user.username, 'full_name': user.full_name, 'created_at': user.created_at}

    # Конвертируем в Pydantic
    user_out = UserOut.model_validate(user_dict)
    # сохраняем пользователя в Redis на время жизни токена
    await redis_client.setex(
        f"user:{user_id}",
        int(ACCESS_TOKEN_EXPIRE_MINUTES * 60),  # Время жизни в секундах
        user_out.model_dump_json()
    )

    return user_out