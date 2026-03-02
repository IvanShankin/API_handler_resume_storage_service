from enum import Enum


class ResumeSortField(str, Enum):
    created_desc = "desc"
    created_asc = "asc"