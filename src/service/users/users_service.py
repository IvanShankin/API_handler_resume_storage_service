from datetime import datetime

from jose import jwt, JWTError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import Users
from src.exeptions.service_exc import IDAlreadyExists, InvalidJWTToken, UserNotFoundServ
from src.repository.database.users import UserRepository
from src.repository.redis.user_cache import UserCacheRepository
from src.schemas.response import UserOut
from src.service.config.schemas import Config


class UserService:

    def __init__(
        self,
        user_repo: UserRepository,
        cache_repo: UserCacheRepository,
        session_db: AsyncSession,
        config: Config,
    ):
        self.user_repo = user_repo
        self.cache_repo = cache_repo
        self.session_db = session_db
        self.conf = config

    async def get_user(self, user_id: int) -> Users | None:
        user_redis = await self.cache_repo.get_user(user_id)
        if not user_redis:
            user_db = await self.user_repo.get_user(user_id=user_id)
            if user_db:
                await self.cache_repo.set_user(user_db)

            return user_db

        return user_redis

    async def get_current_user(self, token: str) -> Users:
        """
        :raise InvalidJWTToken: Невалидный токен
        :raise UserNotFoundServ: Пользователь не найден
        """
        try:
            # Декодируем токен
            payload = jwt.decode(
                token,
                self.conf.env.secret_key,
                algorithms=[self.conf.tokens.algorithm],
                options={"verify_exp": True}
            )

            # Извлекаем ID пользователя
            user_id: int = int(payload.get("sub"))
            if user_id is None:
                raise InvalidJWTToken()

            user = await self.get_user(user_id)
        except JWTError:  # Ловим все ошибки JWT
            raise InvalidJWTToken()

        if user is None:
            raise UserNotFoundServ()

        return user

    async def get_me(self, user: Users) -> UserOut:
        return UserOut(**(user.to_dict()))

    async def create_user(
        self,
        user_id: int,
        username: str,
        full_name: str,
        created_at: datetime,
    ) -> Users:
        """
        :except IDAlreadyExists: Если данный ID занят
        """

        try:
            tx_ctx = self.session_db.begin_nested() if self.session_db.in_transaction() else self.session_db.begin()

            async with tx_ctx:
                user = await self.user_repo.add_user(
                    user_id=user_id,
                    username=username,
                    full_name=full_name,
                    created_at=created_at,
                )

                # flush чтобы поймать IntegrityError здесь
                await self.session_db.flush()

        except IntegrityError as e:
            raise IDAlreadyExists() from e

        await self.cache_repo.set_user(user)
        return user



