"""User schemas and validation helpers."""
from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator

from app.db.models import UserRole


EMAIL_REGEX = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.IGNORECASE)


def _normalize_email(value: str) -> str:
    """Normalize and validate an email address."""
    email = value.strip()
    if not email:
        raise ValueError("Email address cannot be empty")
    if not EMAIL_REGEX.fullmatch(email):
        raise ValueError("Invalid email address format")
    return email.lower()


class UserBase(BaseModel):
    """Base user schema."""

    email: str = Field(..., min_length=3, max_length=255)
    full_name: str = Field(..., min_length=1, max_length=255)
    role: UserRole = UserRole.OPERATOR
    is_active: bool = True

    @field_validator("email")
    @classmethod
    def validate_email(cls, value: str) -> str:
        """Validate and normalize email addresses."""
        return _normalize_email(value)


class UserCreate(UserBase):
    """Schema for creating a user."""

    password: str = Field(..., min_length=8, max_length=100)


class UserUpdate(BaseModel):
    """Schema for updating a user."""

    email: Optional[str] = Field(None, min_length=3, max_length=255)
    full_name: Optional[str] = Field(None, min_length=1, max_length=255)
    role: Optional[UserRole] = None
    is_active: Optional[bool] = None
    password: Optional[str] = Field(None, min_length=8, max_length=100)

    @field_validator("email")
    @classmethod
    def validate_optional_email(cls, value: Optional[str]) -> Optional[str]:
        """Validate email when provided."""
        if value is None:
            return value
        return _normalize_email(value)


class UserResponse(UserBase):
    """Schema for user response."""

    id: int
    created_at: datetime
    updated_at: datetime
    version: int

    model_config = {"from_attributes": True}


class UserLogin(BaseModel):
    """Schema for user login."""

    email: str = Field(..., min_length=3, max_length=255)
    password: str

    @field_validator("email")
    @classmethod
    def validate_login_email(cls, value: str) -> str:
        """Validate login email."""
        return _normalize_email(value)


class Token(BaseModel):
    """Schema for JWT token response."""

    access_token: str
    token_type: str = "bearer"


class TokenData(BaseModel):
    """Schema for token payload data."""

    user_id: int
    email: str
    role: UserRole

    @field_validator("email")
    @classmethod
    def validate_token_email(cls, value: str) -> str:
        """Validate token email payload."""
        return _normalize_email(value)
