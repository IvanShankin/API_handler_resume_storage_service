from pydantic import BaseModel
from enum import Enum

class ProcessingAndRequirementsID(BaseModel):
    processing_id: int
    requirements_id: int

class SortField(str, Enum):
    CREATE_AT = "create_at"  # Сортировка по дате создания
    SCORE = "score"          # Сортировка по проценту совпадения

class SortOrder(str, Enum):
    ASC = "asc"    # По возрастанию
    DESC = "desc"  # По убыванию