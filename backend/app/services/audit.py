"""Audit logging service."""
import json
from typing import Any, Dict, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import AuditLog


class AuditService:
    """Service for audit logging."""

    @staticmethod
    async def log_action(
        db: AsyncSession,
        user_id: Optional[int],
        entity_type: str,
        entity_id: int,
        action: str,
        before: Optional[Dict[str, Any]] = None,
        after: Optional[Dict[str, Any]] = None,
    ) -> AuditLog:
        """Create an audit log entry."""
        audit_log = AuditLog(
            user_id=user_id,
            entity_type=entity_type,
            entity_id=entity_id,
            action=action,
            before_json=json.dumps(before) if before else None,
            after_json=json.dumps(after) if after else None,
        )
        db.add(audit_log)
        await db.flush()
        return audit_log
