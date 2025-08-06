from fastapi import HTTPException, status

class InvalidFileFormat(HTTPException):
    def __init__(self, allowed_extensions):
        super().__init__(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f'Неподдерживаемый формат файла. Разрешены: {', '.join(allowed_extensions)}'
        )

class CorruptedFile(HTTPException):
    def __init__(self, file_name: str):
        super().__init__(status_code=status.HTTP_400_BAD_REQUEST, detail=f'Повреждённый Файл: {file_name}')

class InvalidParameters(HTTPException):
    def __init__(self):
        super().__init__(status_code=status.HTTP_400_BAD_REQUEST, detail=f'Используйте только один параметр: либо processing_id, либо requirements_id')

class EmptyFileException(HTTPException):
    def __init__(self, file_name: str):
        super().__init__(status_code=status.HTTP_400_BAD_REQUEST, detail=f'Файл {file_name} пуст!')

class InvalidCredentialsException(HTTPException):
    def __init__(self):
        super().__init__(status_code=status.HTTP_401_UNAUTHORIZED, detail="Неверный логин или пароль!")

class NoRights(HTTPException):
    def __init__(self):
        super().__init__(status_code=status.HTTP_403_FORBIDDEN,detail="Нет прав на получение этого ресурса!")

class NotFoundData(HTTPException):
    def __init__(self):
        super().__init__(status_code=status.HTTP_404_NOT_FOUND, detail="Запрашиваемые данные не найдены")

class TooManyCharacters(HTTPException):
    def __init__(self, max_length_char: int):
        super().__init__(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Слишком большое количество символов. Максимум: {max_length_char}"
        )

class ToManyRequest(HTTPException):
    def __init__(self, rate_limit_in_seconds: int):
        super().__init__(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Слишком большое количество запросов. Повторите попытку через {rate_limit_in_seconds} секунд"
        )

