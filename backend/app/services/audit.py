"""Audit logging service."""
import json
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import AuditLog


def json_serial(obj: Any) -> Any:
    """JSON serializer for objects not serializable by default json code."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


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
            before_json=json.dumps(before, default=json_serial) if before else None,
            after_json=json.dumps(after, default=json_serial) if after else None,
        )
        db.add(audit_log)
        await db.flush()
        return audit_log
