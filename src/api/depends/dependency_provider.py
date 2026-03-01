from fastapi import Depends
from fastapi.security import OAuth2PasswordBearer

from src.database.models import Users
from src.exeptions.http_exc import InvalidCredentialsException
from src.exeptions.service_exc import InvalidJWTToken, UserNotFoundServ
from src.service.users import UserService, get_users_service


oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="auth/login",
    scheme_name="OAuth2PasswordBearer",
    scopes={"read": "Read access", "write": "Write access"}
)


async def get_current_user(
    token: str = Depends(oauth2_scheme),
    user_service: UserService = Depends(get_users_service)
) -> Users:
    try:
        return await user_service.get_current_user(token)
    except (InvalidJWTToken, UserNotFoundServ):
        raise InvalidCredentialsException()

