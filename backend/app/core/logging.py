"""Logging configuration for DCDock backend."""
import logging
import sys
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler
from typing import Optional

from app.core.config import get_settings

settings = get_settings()


def setup_logging(log_level: Optional[str] = None) -> logging.Logger:
    """
    Configure application logging.

    Sets up both console and file logging with appropriate formatters.
    File logs are rotated daily and kept for 30 days.

    Args:
        log_level: Optional log level override (DEBUG, INFO, WARNING, ERROR, CRITICAL)
                  If not provided, uses DEBUG in debug mode, INFO otherwise.

    Returns:
        Root logger instance
    """
    # Determine log level
    if log_level:
        level = getattr(logging, log_level.upper())
    else:
        level = logging.DEBUG if settings.debug else logging.INFO

    # Create logs directory
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Root logger configuration
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    root_logger.handlers.clear()

    # Console Handler - for development and immediate feedback
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)

    if settings.debug:
        # Detailed format for development
        console_formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    else:
        # Simpler format for production console
        console_formatter = logging.Formatter(
            fmt="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # File Handler - rotating daily, keep 30 days
    file_handler = TimedRotatingFileHandler(
        filename=log_dir / "dcdock.log",
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.INFO)

    # Detailed format for file logs
    file_formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

    # Error File Handler - separate file for errors only
    error_handler = TimedRotatingFileHandler(
        filename=log_dir / "dcdock_errors.log",
        when="midnight",
        interval=1,
        backupCount=90,  # Keep errors longer (90 days)
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_formatter)
    root_logger.addHandler(error_handler)

    # Configure third-party loggers to reduce noise
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.error").setLevel(logging.INFO)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    # Log startup message
    root_logger.info(f"Logging initialized - Level: {logging.getLevelName(level)}")
    root_logger.info(f"Debug mode: {settings.debug}")
    root_logger.info(f"Log directory: {log_dir.absolute()}")

    return root_logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger for a specific module.

    Args:
        name: Module name (typically __name__)

    Returns:
        Logger instance for the module
    """
    return logging.getLogger(name)
