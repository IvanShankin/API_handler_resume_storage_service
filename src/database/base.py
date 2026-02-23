from sqlalchemy import inspect
from sqlalchemy.orm import declarative_base

Base_sqlalchemy = declarative_base()

class Base(Base_sqlalchemy):
    __abstract__ = True  # указывает что класс не будет таблицей

    def to_dict(self):
        """преобразует в словарь все колонки у выбранного объекта"""
        return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}