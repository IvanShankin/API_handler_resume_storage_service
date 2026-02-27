
class ServiceException(Exception):
    pass


class NoRightsService(ServiceException):
    pass


class ResourceNotFound(ServiceException):
    pass


class IDAlreadyExists(ServiceException):
    pass


class InvalidJWTToken(ServiceException):
    pass


class UserNotFoundServ(ServiceException):
    pass


class InsertionErrorService(ServiceException):
    pass


class RedisError(ServiceException):
    pass


class DatabaseError(ServiceException):
    pass