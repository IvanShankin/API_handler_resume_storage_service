import enum

from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, ARRAY, Boolean, func, Enum
from sqlalchemy.orm import relationship

from src.database.base import Base


class ProcessingStatus(enum.Enum):
    IN_PROGRESS = "in_progress"
    SUCCESSFULLY = "successfully"
    FAILED = "failed"


class Users(Base):
    __tablename__ = "users"
    user_id = Column(Integer, primary_key=True, nullable=False)
    username = Column(String(255), unique=True, nullable=False)
    full_name = Column(String(255), nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False)

    resume = relationship("Resumes", back_populates="user")
    requirements = relationship("Requirements", back_populates="user")
    processing = relationship("Processing", back_populates="user")


class Resumes(Base):
    __tablename__ = "resumes"
    resume_id = Column(Integer, primary_key=True, nullable=False)
    user_id = Column(Integer, ForeignKey('users.user_id'), nullable=False)
    requirement_id = Column(Integer, ForeignKey('requirements.requirement_id'), nullable=False)
    resume = Column(String(15000), nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    user = relationship("Users", back_populates="resume")
    requirement = relationship("Requirements", back_populates="resume")
    processing = relationship("Processing", back_populates="resume")


class Requirements(Base):
    __tablename__ = "requirements"
    requirement_id = Column(Integer, primary_key=True, nullable=False)
    user_id = Column(Integer, ForeignKey('users.user_id'), nullable=False)
    requirements = Column(String(5000), nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    user = relationship("Users", back_populates="requirements")
    resume = relationship("Resumes", back_populates="requirement")
    processing = relationship("Processing", back_populates="requirements")


class Processing(Base):
    __tablename__ = "processing"
    processing_id = Column(Integer, primary_key=True, nullable=False)
    resume_id = Column(Integer, ForeignKey('resumes.resume_id'), nullable=False)
    requirement_id = Column(Integer, ForeignKey('requirements.requirement_id'), nullable=False)
    user_id = Column(Integer, ForeignKey('users.user_id'), nullable=False)

    status = Column(
        Enum(
            ProcessingStatus,
            values_callable=lambda x: [e.value for e in x],
            name="account_status"
        ),
        nullable=False
    )

    success = Column(Boolean, nullable=False)

    # только при success == False
    message_error = Column(String, nullable=True)
    wait_seconds = Column(Integer, nullable=True)

    # только при success == True
    score = Column(Integer, nullable=True)
    matches = Column(ARRAY(String), nullable=True)  # перечисление навыков которые совпадают требованию
    recommendation = Column(String(700), nullable=True)  # рекомендации по найму
    verdict = Column(String(50), nullable=True)  # заключение "Подходит" или "Не подходит"

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    user = relationship("Users", back_populates="processing")
    resume = relationship("Resumes", back_populates="processing")
    requirements = relationship("Requirements", back_populates="processing")

