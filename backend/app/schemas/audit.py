"""Audit log schemas."""
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class AuditLogResponse(BaseModel):
    """Schema for audit log response."""

    id: int
    user_id: Optional[int]
    entity_type: str
    entity_id: int
    action: str
    before_json: Optional[str]
    after_json: Optional[str]
    created_at: datetime

    model_config = {"from_attributes": True}
