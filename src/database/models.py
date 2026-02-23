from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, ARRAY, inspect, Boolean, func
from sqlalchemy.orm import relationship

from src.database.base import Base
from src.config import MAX_CHAR_RESUME, MAX_CHAR_REQUIREMENTS

class User(Base):
    __tablename__ = "users"
    user_id = Column(Integer, primary_key=True, nullable=False)
    username = Column(String(255), unique=True, nullable=False)
    full_name = Column(String(255), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)

    resume = relationship("Resume", back_populates="user")
    requirements = relationship("Requirements", back_populates="user")
    processing = relationship("Processing", back_populates="user")

class Resume(Base):
    __tablename__ = "resume"
    resume_id = Column(Integer, primary_key=True, nullable=False)
    user_id = Column(Integer, ForeignKey('users.user_id'), nullable=False)
    requirement_id = Column(Integer, ForeignKey('requirements.requirements_id'), nullable=False)
    resume = Column(String(MAX_CHAR_RESUME), nullable=False)

    user = relationship("User", back_populates="resume")
    requirement = relationship("Requirements", back_populates="resume")
    processing = relationship("Processing", back_populates="resume")

class Requirements(Base):
    __tablename__ = "requirements"
    requirements_id = Column(Integer, primary_key=True, nullable=False)
    user_id = Column(Integer, ForeignKey('users.user_id'), nullable=False)
    requirements = Column(String(MAX_CHAR_REQUIREMENTS), nullable=False)

    user = relationship("User", back_populates="requirements")
    resume = relationship("Resume", back_populates="requirement")
    processing = relationship("Processing", back_populates="requirements")

    def to_dict(self):
        """преобразует в словарь все колонки у выбранного объекта"""
        return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}

class Processing(Base):
    __tablename__ = "processing"
    processing_id = Column(Integer, primary_key=True, nullable=False)
    resume_id = Column(Integer, ForeignKey('resume.resume_id'), nullable=False)
    requirements_id = Column(Integer, ForeignKey('requirements.requirements_id'), nullable=False)
    user_id = Column(Integer, ForeignKey('users.user_id'), nullable=False)

    success = Column(Boolean, nullable=False)
    message_error = Column(String, nullable=True) # только при success == False
    wait_seconds = Column(Integer, nullable=True) # только при success == False

    score = Column(Integer, nullable=True)
    matches = Column(ARRAY(String), nullable=True)  # перечисление навыков которые совпадают требованию
    recommendation = Column(String(700), nullable=True)  # рекомендации по найму
    verdict = Column(String(50), nullable=True)  # заключение "Подходит" или "Не подходит"

    create_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    user = relationship("User", back_populates="processing")
    resume = relationship("Resume", back_populates="processing")
    requirements = relationship("Requirements", back_populates="processing")

    def to_dict(self):
        """преобразует в словарь все колонки у выбранного объекта"""
        return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}
