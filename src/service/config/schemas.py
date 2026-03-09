import os
from datetime import timedelta
from pathlib import Path
from typing import List

from pydantic import BaseModel, ConfigDict
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncEngine
from sqlalchemy.orm import sessionmaker

from src.service.config.base import init_env


class Config:
    def __init__(self):
        init_env()

        self.min_commit_count_kafka: int = 10
        self.login_block_time: timedelta = timedelta(seconds=200) # Период блокировки при частых попытках войти

        self.env = EnvConfig.build()
        self.db_connection = DbConnectionConfig.build(self.env)
        self.paths = PathsConfig.build()
        self.tokens = TokensConfig.build()
        self.kafka_topics = KafkaTopics.build()
        self.lifespan_redis = LifespanInRedis.build()


class EnvConfig(BaseModel):
    secret_key: str

    db_host: str
    db_port: str
    db_user: str
    db_password: str
    db_name: str

    kafka_bootstrap_servers: str

    redis_host: str
    redis_port: int

    mode: str

    @classmethod
    def build(cls) -> "EnvConfig":
        return cls(
            secret_key=os.environ['SECRET_KEY'],
            db_host=os.environ['DB_HOST'],
            db_port=os.environ['DB_PORT'],
            db_user=os.environ['DB_USER'],
            db_password=os.environ['DB_PASSWORD'],
            db_name=os.environ['DB_NAME'],

            kafka_bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'],

            redis_host=os.environ['REDIS_HOST'],
            redis_port=int(os.environ['REDIS_PORT']),

            mode=os.environ['MODE']
        )


class DbConnectionConfig(BaseModel):
    postgres_server_url: str  # URL для подключения к серверу PostgresSQL без указания конкретной базы данных
    sql_db_url: str
    engine: AsyncEngine
    session_local: sessionmaker

    model_config = ConfigDict(
        arbitrary_types_allowed=True  # Разрешаем произвольные типы
    )

    @classmethod
    def build(cls, conf_env: EnvConfig) -> "DbConnectionConfig":
        sql_db_url = f'postgresql+asyncpg://{conf_env.db_user}:{conf_env.db_password}@{conf_env.db_host}:{conf_env.db_port}/{conf_env.db_name}'
        engine = create_async_engine(sql_db_url)

        return cls(
            postgres_server_url=f'postgresql+asyncpg://{conf_env.db_user}:{conf_env.db_password}@{conf_env.db_host}:{conf_env.db_port}/postgres',
            sql_db_url=sql_db_url,
            engine=engine,
            session_local=sessionmaker(
                engine,
                class_=AsyncSession,
                expire_on_commit=False,
                autoflush=False
            )
        )


class PathsConfig(BaseModel):
    base: Path
    media: Path
    log_dir: Path
    log_file: Path

    @classmethod
    def build(cls) -> "PathsConfig":
        base = Path(__file__).resolve().parents[3]
        media = base / Path("media")
        log_dir = media / "logs"
        log_file = log_dir / "auth_service.log"

        media.mkdir(exist_ok=True)
        log_dir.mkdir(exist_ok=True)

        return cls(
            base=base,
            media=media,
            log_dir=log_dir,
            log_file=log_file,
        )


class TokensConfig(BaseModel):
    access_token_expire_minutes: int
    refresh_token_expire_days: int
    algorithm: str

    @classmethod
    def build(cls) -> "TokensConfig":
        return cls(
            access_token_expire_minutes=30,
            refresh_token_expire_days=30,
            algorithm="HS256",
        )



class KafkaTopics(BaseModel):
    user_created: str
    resume_created: str
    requirements_created: str
    processing_created: str

    processing_finished: str

    processing_deleted: str
    resumes_deleted: str
    requirements_deleted: str

    all_topics: List[str]

    @classmethod
    def build(cls) -> "KafkaTopics":
        return cls(
            user_created='user.created',
            resume_created='resume.created',
            requirements_created='requirement.created',
            processing_created='processing.requested',

            processing_finished="processing.finished",

            processing_deleted='processing.deleted',
            resumes_deleted='resume.deleted',
            requirements_deleted='requirement.deleted',

            all_topics=[
                'user.created', 'resume.created', 'requirement.created', 'processing.requested', 'processing.finished',
                'processing.deleted', 'resume.deleted', 'requirement.deleted',
            ]
        )


class LifespanInRedis(BaseModel):
    """Время жизни данных в Redis в секундах"""

    user: int
    resume_by_requirement: int
    processing_by_resume: int
    requirement_by_user: int

    kafka_message: int

    @classmethod
    def build(cls) -> "LifespanInRedis":
        return cls(
            user=int(timedelta(days=1).total_seconds()),
            resume_by_requirement=int(timedelta(days=1).total_seconds()),
            processing_by_resume=int(timedelta(days=1).total_seconds()),
            requirement_by_user=int(timedelta(days=3).total_seconds()),
            kafka_message=int(timedelta(hours=5).total_seconds()),
        )