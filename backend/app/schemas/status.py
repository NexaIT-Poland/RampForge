"""Status schemas."""
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class StatusBase(BaseModel):
    """Base status schema."""

    code: str = Field(..., min_length=1, max_length=50)
    label: str = Field(..., min_length=1, max_length=100)
    color: str = Field(..., min_length=1, max_length=50)
    sort_order: int = Field(default=0, ge=0)


class StatusCreate(StatusBase):
    """Schema for creating a status."""

    pass


class StatusUpdate(BaseModel):
    """Schema for updating a status."""

    code: Optional[str] = Field(None, min_length=1, max_length=50)
    label: Optional[str] = Field(None, min_length=1, max_length=100)
    color: Optional[str] = Field(None, min_length=1, max_length=50)
    sort_order: Optional[int] = Field(None, ge=0)


class StatusResponse(StatusBase):
    """Schema for status response."""

    id: int
    created_at: datetime
    updated_at: datetime
    version: int

    model_config = {"from_attributes": True}
