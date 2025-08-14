import os
import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from dotenv import load_dotenv

from srt.database.base import Base

load_dotenv()  # Загружает переменные из .env
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# URL для подключения к серверу PostgreSQL без указания конкретной базы данных
POSTGRES_SERVER_URL = f'postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/postgres'
# postgresql+asyncpg это означает, что БД работает в асинхронном режиме
SQL_DB_URL = f'postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'

engine_for_create = create_async_engine(SQL_DB_URL)

engine = create_async_engine(SQL_DB_URL)
session_local = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False
)

async def get_db() -> AsyncSession:
    async with session_local() as db:  # Автоматическое закрытие сессии
        yield db


async def create_database():
    """Создает базу данных и все таблицы в ней (если существует, то ничего не произойдёт) """
    # Сначала подключаемся к серверу PostgreSQL без указания конкретной базы
    engine = create_async_engine(POSTGRES_SERVER_URL, isolation_level="AUTOCOMMIT")
    try:
        # Проверяем существование базы данных и создаем если ее нет
        async with engine.connect() as conn:
            result = await conn.execute(
                text(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")
            )
            database_exists = result.scalar() == 1

            if not database_exists: # если БД нет
                logging.info(f"Creating database {DB_NAME}...")
                await conn.execute(text(f"CREATE DATABASE {DB_NAME}"))
                logging.info(f"Database {DB_NAME} created successfully")
            else:
                logging.info(f"Database {DB_NAME} already exists")
    except Exception as e:
        logging.error(f"Error checking/creating database: {e}")
        raise
    finally:
        await engine.dispose()

    # создаем таблицы в целевой базе данных
    engine = create_async_engine(SQL_DB_URL)
    try:
        async with engine.begin() as conn:
            logging.info("Creating database tables...")
            await conn.run_sync(Base.metadata.create_all)
            logging.info("Database tables created successfully")
    except Exception as e:
        logging.error(f"Error creating tables: {e}")
        raise
    finally:
        await engine.dispose()
