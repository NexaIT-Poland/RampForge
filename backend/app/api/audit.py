"""Audit log API routes."""
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_current_active_user
from app.db.models import AuditLog, User
from app.db.session import get_db
from app.schemas.audit import AuditLogResponse

router = APIRouter(prefix="/audit", tags=["audit"])


@router.get("/", response_model=List[AuditLogResponse])
async def list_audit_logs(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    entity_id: Optional[int] = Query(None, description="Filter by entity ID"),
    action: Optional[str] = Query(None, description="Filter by action (CREATE, UPDATE, DELETE)"),
    skip: int = 0,
    limit: int = 100,
) -> List[AuditLogResponse]:
    """List audit logs with optional filters."""
    query = select(AuditLog).order_by(AuditLog.created_at.desc())

    if entity_type:
        query = query.where(AuditLog.entity_type == entity_type)
    if entity_id:
        query = query.where(AuditLog.entity_id == entity_id)
    if action:
        query = query.where(AuditLog.action == action)

    result = await db.execute(query.offset(skip).limit(limit))
    logs = result.scalars().all()
    return [AuditLogResponse.model_validate(log) for log in logs]
