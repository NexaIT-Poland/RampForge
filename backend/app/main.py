"""Main FastAPI application."""
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.api import assignments, audit, auth, loads, ramps, statuses, users, websocket
from app.core.config import get_settings
from app.core.limiter import limiter
from app.core.logging import get_logger, setup_logging
from app.db.migrations import run_migrations
from app.db.session import AsyncSessionLocal, init_db

settings = get_settings()
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan handler."""
    # Startup
    # Initialize logging first
    setup_logging()
    logger.info(f"Starting {settings.app_name} v{settings.app_version}")

    # Initialize database
    await init_db()
    logger.info("Database initialized")

    # Run database migrations
    async with AsyncSessionLocal() as session:
        await run_migrations(session)
        logger.info("Database migrations completed")

    logger.info("Application startup complete")

    yield

    # Shutdown
    logger.info("Application shutting down")
    pass


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    debug=settings.debug,
    lifespan=lifespan,
)

# Rate limiter setup
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth.router, prefix="/api")
app.include_router(users.router, prefix="/api")
app.include_router(ramps.router, prefix="/api")
app.include_router(statuses.router, prefix="/api")
app.include_router(loads.router, prefix="/api")
app.include_router(assignments.router, prefix="/api")
app.include_router(audit.router, prefix="/api")
app.include_router(websocket.router, prefix="/api")


@app.get("/")
async def root() -> dict[str, str]:
    """Root endpoint."""
    return {"message": f"Welcome to {settings.app_name} v{settings.app_version}"}


@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "healthy"}
