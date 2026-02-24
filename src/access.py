import redis
from fastapi import Depends
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import ValidationError

from src.database.core import get_db
from src.database.models import User
from src.dependencies.redis_dependencies import get_redis
from src.exeptions.http_exc import InvalidCredentialsException
from src.schemas.response import UserOut
from src.service.config import get_config


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
    conf = get_config()
    try:
        # Декодируем токен
        payload = jwt.decode(
            token,
            conf.env.secret_key,
            algorithms=[conf.tokens.algorithm],
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
        int(conf.tokens.access_token_expire_minutes * 60),  # Время жизни в секундах
        user_out.model_dump_json()
    )

    return user_out