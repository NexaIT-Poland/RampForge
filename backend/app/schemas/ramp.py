"""Ramp schemas."""
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from app.db.models import LoadDirection


class RampBase(BaseModel):
    """Base ramp schema."""

    code: str = Field(..., min_length=1, max_length=50)
    description: Optional[str] = Field(None, max_length=255)
    direction: LoadDirection


class RampCreate(RampBase):
    """Schema for creating a ramp."""

    pass


class RampUpdate(BaseModel):
    """Schema for updating a ramp."""

    code: Optional[str] = Field(None, min_length=1, max_length=50)
    description: Optional[str] = Field(None, max_length=255)
    direction: Optional[LoadDirection] = None


class RampResponse(RampBase):
    """Schema for ramp response."""

    id: int
    created_at: datetime
    updated_at: datetime
    version: int

    model_config = {"from_attributes": True}
