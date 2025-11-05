"""Database session management."""
from typing import AsyncGenerator

from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError
from sqlalchemy.exc import (
    DatabaseError,
    DataError,
    IntegrityError,
    OperationalError,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import get_settings
from app.core.logging import get_logger

settings = get_settings()
logger = get_logger(__name__)

# Create async engine
engine = create_async_engine(
    settings.database_url,
    echo=settings.debug,
    future=True,
    # SQLite specific settings
    connect_args={"check_same_thread": False} if settings.is_sqlite else {},
)

# Create async session factory
AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency for getting async database sessions.

    Yields:
        AsyncSession: Database session

    Raises:
        HTTPException: Application-level errors (401, 403, 404) - passes through
        RequestValidationError: Pydantic validation errors (422) - passes through
        IntegrityError: On constraint violations
        OperationalError: On connection/database errors
        DataError: On invalid data
        DatabaseError: On general database errors
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except HTTPException:
            # HTTPException is application-level error (e.g., 401, 403, 404)
            # Not a database error - let it pass through without rollback or logging
            await session.rollback()
            raise
        except RequestValidationError:
            # RequestValidationError is Pydantic validation error (e.g., 422 Unprocessable Entity)
            # This is normal input validation, not a database error
            await session.rollback()
            raise
        except IntegrityError as e:
            await session.rollback()
            logger.error(f"Database integrity error: {e}", exc_info=True)
            raise
        except OperationalError as e:
            await session.rollback()
            logger.error(f"Database operational error: {e}", exc_info=True)
            raise
        except DataError as e:
            await session.rollback()
            logger.error(f"Database data error: {e}", exc_info=True)
            raise
        except DatabaseError as e:
            await session.rollback()
            logger.error(f"Database error: {e}", exc_info=True)
            raise
        except Exception as e:
            await session.rollback()
            logger.critical(
                f"Unexpected error in database session: {e}",
                exc_info=True
            )
            raise
        finally:
            await session.close()


async def init_db() -> None:
    """Initialize database tables."""
    from app.db.base import Base

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
