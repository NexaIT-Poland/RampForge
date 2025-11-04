"""Load schemas."""
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from app.db.models import LoadDirection


class LoadBase(BaseModel):
    """Base load schema."""

    reference: str = Field(..., min_length=1, max_length=100)
    direction: LoadDirection
    planned_arrival: Optional[datetime] = None
    planned_departure: Optional[datetime] = None
    notes: Optional[str] = None


class LoadCreate(LoadBase):
    """Schema for creating a load."""

    pass


class LoadUpdate(BaseModel):
    """Schema for updating a load."""

    reference: Optional[str] = Field(None, min_length=1, max_length=100)
    direction: Optional[LoadDirection] = None
    planned_arrival: Optional[datetime] = None
    planned_departure: Optional[datetime] = None
    notes: Optional[str] = None


class LoadResponse(LoadBase):
    """Schema for load response."""

    id: int
    created_at: datetime
    updated_at: datetime
    version: int

    model_config = {"from_attributes": True}
