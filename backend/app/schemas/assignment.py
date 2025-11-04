"""Assignment schemas."""
from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from app.schemas.load import LoadResponse
from app.schemas.ramp import RampResponse
from app.schemas.status import StatusResponse
from app.schemas.user import UserResponse


class AssignmentBase(BaseModel):
    """Base assignment schema."""

    ramp_id: int
    load_id: int
    status_id: int
    eta_in: Optional[datetime] = None
    eta_out: Optional[datetime] = None


class AssignmentCreate(AssignmentBase):
    """Schema for creating an assignment."""

    pass


class AssignmentUpdate(BaseModel):
    """Schema for updating an assignment."""

    ramp_id: Optional[int] = None
    load_id: Optional[int] = None
    status_id: Optional[int] = None
    eta_in: Optional[datetime] = None
    eta_out: Optional[datetime] = None
    version: int  # Required for optimistic locking


class AssignmentResponse(AssignmentBase):
    """Schema for assignment response."""

    id: int
    created_by: int
    updated_by: int
    created_at: datetime
    updated_at: datetime
    version: int

    model_config = {"from_attributes": True}


class AssignmentDetailResponse(AssignmentResponse):
    """Schema for detailed assignment response with relationships."""

    ramp: RampResponse
    load: LoadResponse
    status: StatusResponse
    creator: UserResponse
    updater: UserResponse

    model_config = {"from_attributes": True}


class ConflictError(BaseModel):
    """Schema for version conflict error."""

    detail: str = "Version conflict detected"
    current_version: int
    provided_version: int
    current_data: dict
