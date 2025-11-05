"""Audit logging service."""
import json
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import AuditLog


def json_serial(obj: Any) -> Any:
    """
    JSON serializer for objects not serializable by default json code.

    Handles datetime serialization to ISO format. Used as the 'default' parameter
    for json.dumps() when serializing audit log snapshots.

    Args:
        obj: Object to serialize

    Returns:
        Serialized representation of the object

    Raises:
        TypeError: If object type is not supported for serialization
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


class AuditService:
    """
    Service for audit logging.

    Provides functionality to log changes to entities (CREATE, UPDATE, DELETE actions)
    with before/after snapshots. Logs are stored in the audit_log table with JSON
    serialization of entity data for historical tracking and compliance.
    """

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
        """
        Create an audit log entry.

        Records an action performed on an entity with optional before/after snapshots.
        Before/after data is serialized to JSON with datetime handling. The log entry
        is flushed to the database but not committed (caller must commit).

        Args:
            db: Database session
            user_id: ID of user who performed the action (None for system actions)
            entity_type: Type of entity (e.g., "user", "ramp", "assignment")
            entity_id: ID of the affected entity
            action: Action performed ("CREATE", "UPDATE", "DELETE")
            before: Entity state before the action (for UPDATE/DELETE)
            after: Entity state after the action (for CREATE/UPDATE)

        Returns:
            AuditLog: Created audit log entry

        Example:
            >>> await AuditService.log_action(
            ...     db=db,
            ...     user_id=current_user.id,
            ...     entity_type="assignment",
            ...     entity_id=123,
            ...     action="UPDATE",
            ...     before={"status_id": 1, "version": 3},
            ...     after={"status_id": 2, "version": 4}
            ... )
        """
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
