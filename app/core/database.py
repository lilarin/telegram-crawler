from typing import Any, AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from app.config import config

engine = create_async_engine(
    config.DATABASE_URL,
    echo=False,
    pool_size=100,
    max_overflow=100,
    pool_timeout=60,
    pool_pre_ping=True,
)


async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

Base = declarative_base()


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_db() -> AsyncGenerator[AsyncSession | Any, Any]:
    async with async_session() as session:
        try:
            yield session
        finally:
            await session.close()
