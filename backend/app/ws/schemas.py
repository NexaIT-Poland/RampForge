"""WebSocket message schemas."""
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel


class WSMessageType(str, Enum):
    """WebSocket message types."""

    # Server to client
    ASSIGNMENT_CREATED = "assignment_created"
    ASSIGNMENT_UPDATED = "assignment_updated"
    ASSIGNMENT_DELETED = "assignment_deleted"
    CONFLICT_DETECTED = "conflict_detected"
    CONNECTION_ACK = "connection_ack"
    ERROR = "error"

    # Client to server
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    PING = "ping"


class WSMessage(BaseModel):
    """Base WebSocket message."""

    type: WSMessageType
    timestamp: datetime = datetime.utcnow()
    data: Optional[Dict[str, Any]] = None


class WSSubscribeMessage(BaseModel):
    """Subscribe to updates message."""

    type: WSMessageType = WSMessageType.SUBSCRIBE
    filters: Optional[Dict[str, Any]] = None  # e.g., {"direction": "IB"}


class WSConnectionAck(BaseModel):
    """Connection acknowledgment message."""

    type: WSMessageType = WSMessageType.CONNECTION_ACK
    timestamp: datetime
    client_id: str
    message: str = "Connected to DCDock real-time updates"


class WSAssignmentUpdate(BaseModel):
    """Assignment update notification."""

    type: WSMessageType
    timestamp: datetime
    assignment_id: int
    action: str  # CREATE, UPDATE, DELETE
    user_id: int
    user_email: str
    data: Dict[str, Any]  # Full assignment data


class WSConflictNotification(BaseModel):
    """Conflict detection notification."""

    type: WSMessageType = WSMessageType.CONFLICT_DETECTED
    timestamp: datetime
    assignment_id: int
    current_version: int
    attempted_version: int
    current_data: Dict[str, Any]
    message: str = "Another user has modified this assignment"


class WSError(BaseModel):
    """Error message."""

    type: WSMessageType = WSMessageType.ERROR
    timestamp: datetime
    message: str
    details: Optional[str] = None
