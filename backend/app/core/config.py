"""Application configuration."""
from functools import lru_cache
from secrets import token_urlsafe
from typing import List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application
    app_name: str = "DCDock API"
    app_version: str = "0.1.0"
    debug: bool = False

    # Database
    database_url: str = "sqlite+aiosqlite:///./dcdock.db"

    # Security
    secret_key: str = Field(default_factory=lambda: token_urlsafe(32))
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 1440  # 24 hours

    # CORS
    cors_origins: List[str] = ["http://localhost:8000"]

    @field_validator("cors_origins", mode="before")
    @classmethod
    def parse_cors_origins(cls, v: str | List[str]) -> List[str]:
        """Parse CORS origins from string or list."""
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",")]
        return v

    # Server
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    @property
    def is_sqlite(self) -> bool:
        """Check if database is SQLite."""
        return "sqlite" in self.database_url.lower()

    @property
    def is_postgresql(self) -> bool:
        """Check if database is PostgreSQL."""
        return "postgresql" in self.database_url.lower()


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
