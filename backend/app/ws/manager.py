"""WebSocket connection manager."""
import asyncio
import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from fastapi import WebSocket, WebSocketDisconnect
from pydantic import ValidationError

from app.core.logging import get_logger
from app.db.models import LoadDirection
from app.ws.schemas import (
    WSAssignmentUpdate,
    WSConnectionAck,
    WSConflictNotification,
    WSError,
    WSMessage,
    WSMessageType,
    WSSubscribeMessage,
)

logger = get_logger(__name__)


class ConnectionManager:
    """Manages WebSocket connections and broadcasts."""

    def __init__(self) -> None:
        """Initialize connection manager."""
        self.active_connections: Dict[str, WebSocket] = {}
        self.client_filters: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, client_id: Optional[str] = None) -> str:
        """
        Accept a new WebSocket connection.

        Args:
            websocket: The WebSocket connection
            client_id: Optional client identifier

        Returns:
            Generated or provided client_id
        """
        await websocket.accept()

        if client_id is None:
            client_id = str(uuid.uuid4())

        async with self._lock:
            self.active_connections[client_id] = websocket
            self.client_filters[client_id] = {}

        # Send connection acknowledgment
        ack = WSConnectionAck(
            timestamp=datetime.utcnow(),
            client_id=client_id,
        )
        await self._send_to_client(client_id, ack.model_dump(mode="json"))

        return client_id

    async def disconnect(self, client_id: str) -> None:
        """
        Remove a client connection.

        Args:
            client_id: Client identifier
        """
        async with self._lock:
            self.active_connections.pop(client_id, None)
            self.client_filters.pop(client_id, None)

    async def handle_client_message(
        self, client_id: str, message_text: str
    ) -> Optional[Dict[str, Any]]:
        """
        Handle incoming message from client.

        Args:
            client_id: Client identifier
            message_text: Raw message text

        Returns:
            Response message if applicable
        """
        try:
            message_data = json.loads(message_text)
            message_type = message_data.get("type")

            if message_type == WSMessageType.SUBSCRIBE:
                # Handle subscription with filters
                subscribe_msg = WSSubscribeMessage(**message_data)
                async with self._lock:
                    self.client_filters[client_id] = subscribe_msg.filters or {}
                return {
                    "type": "subscribe_ack",
                    "message": "Subscription updated",
                    "filters": self.client_filters[client_id],
                }

            elif message_type == WSMessageType.UNSUBSCRIBE:
                # Clear filters
                async with self._lock:
                    self.client_filters[client_id] = {}
                return {"type": "unsubscribe_ack", "message": "Unsubscribed from all updates"}

            elif message_type == WSMessageType.PING:
                return {"type": "pong", "timestamp": datetime.utcnow().isoformat()}

            else:
                return {
                    "type": WSMessageType.ERROR,
                    "message": f"Unknown message type: {message_type}",
                }

        except json.JSONDecodeError as e:
            logger.warning(f"Invalid JSON from client {client_id}: {message_text[:100]}")
            return {"type": WSMessageType.ERROR, "message": "Invalid JSON"}
        except ValidationError as e:
            logger.warning(f"Invalid message format from client {client_id}: {e}")
            return {"type": WSMessageType.ERROR, "message": "Invalid message format", "details": str(e)}
        except KeyError as e:
            logger.warning(f"Missing required field in message from client {client_id}: {e}")
            return {"type": WSMessageType.ERROR, "message": f"Missing required field: {e}"}
        except Exception as e:
            logger.critical(
                f"Unexpected error processing message from client {client_id}: {e}",
                exc_info=True,
                extra={"client_id": client_id, "message": message_text[:200]}
            )
            return {"type": WSMessageType.ERROR, "message": "Internal server error"}

    async def broadcast_assignment_update(
        self,
        assignment_id: int,
        action: str,
        user_id: int,
        user_email: str,
        assignment_data: Dict[str, Any],
    ) -> None:
        """
        Broadcast assignment update to all connected clients.

        Args:
            assignment_id: Assignment ID
            action: Action type (CREATE, UPDATE, DELETE)
            user_id: User who performed the action
            user_email: User's email
            assignment_data: Full assignment data including relationships
        """
        # Determine message type
        if action == "CREATE":
            msg_type = WSMessageType.ASSIGNMENT_CREATED
        elif action == "UPDATE":
            msg_type = WSMessageType.ASSIGNMENT_UPDATED
        elif action == "DELETE":
            msg_type = WSMessageType.ASSIGNMENT_DELETED
        else:
            msg_type = WSMessageType.ASSIGNMENT_UPDATED

        message = WSAssignmentUpdate(
            type=msg_type,
            timestamp=datetime.utcnow(),
            assignment_id=assignment_id,
            action=action,
            user_id=user_id,
            user_email=user_email,
            data=assignment_data,
        )

        await self._broadcast_filtered(message.model_dump(mode="json"), assignment_data)

    async def broadcast_conflict(
        self,
        assignment_id: int,
        current_version: int,
        attempted_version: int,
        current_data: Dict[str, Any],
    ) -> None:
        """
        Broadcast conflict notification.

        Args:
            assignment_id: Assignment ID
            current_version: Current version in database
            attempted_version: Version that was attempted
            current_data: Current assignment data
        """
        message = WSConflictNotification(
            timestamp=datetime.utcnow(),
            assignment_id=assignment_id,
            current_version=current_version,
            attempted_version=attempted_version,
            current_data=current_data,
        )

        await self._broadcast_filtered(message.model_dump(mode="json"), current_data)

    async def _broadcast_filtered(
        self, message: Dict[str, Any], assignment_data: Dict[str, Any]
    ) -> None:
        """
        Broadcast message to clients matching filters.

        Args:
            message: Message to broadcast
            assignment_data: Assignment data for filter matching
        """
        # Extract load direction if available
        load_direction: Optional[str] = None
        if "load" in assignment_data and isinstance(assignment_data["load"], dict):
            load_direction = assignment_data["load"].get("direction")

        disconnected_clients: Set[str] = set()

        async with self._lock:
            for client_id, websocket in self.active_connections.items():
                # Check if client's filters match
                filters = self.client_filters.get(client_id, {})

                # Apply direction filter if set
                if filters.get("direction") and load_direction:
                    if filters["direction"] != load_direction:
                        continue  # Skip this client

                try:
                    await websocket.send_json(message)
                except WebSocketDisconnect:
                    logger.debug(f"Client {client_id} disconnected during broadcast")
                    disconnected_clients.add(client_id)
                except (ConnectionError, RuntimeError) as e:
                    logger.error(
                        f"Connection error sending to client {client_id}: {e}",
                        exc_info=True,
                        extra={"client_id": client_id}
                    )
                    disconnected_clients.add(client_id)
                except Exception as e:
                    logger.critical(
                        f"Unexpected error broadcasting to client {client_id}: {e}",
                        exc_info=True,
                        extra={"client_id": client_id, "message_type": message.get("type")}
                    )
                    disconnected_clients.add(client_id)

        # Clean up disconnected clients
        for client_id in disconnected_clients:
            await self.disconnect(client_id)

    async def _send_to_client(self, client_id: str, message: Dict[str, Any]) -> None:
        """
        Send message to specific client.

        Args:
            client_id: Client identifier
            message: Message to send
        """
        websocket = self.active_connections.get(client_id)
        if websocket:
            try:
                await websocket.send_json(message)
            except WebSocketDisconnect:
                logger.debug(f"Client {client_id} disconnected while sending message")
                await self.disconnect(client_id)
            except (ConnectionError, RuntimeError) as e:
                logger.error(
                    f"Connection error sending to client {client_id}: {e}",
                    exc_info=True,
                    extra={"client_id": client_id}
                )
                await self.disconnect(client_id)
            except Exception as e:
                logger.critical(
                    f"Unexpected error sending to client {client_id}: {e}",
                    exc_info=True,
                    extra={"client_id": client_id, "message_type": message.get("type")}
                )
                await self.disconnect(client_id)

    def get_connection_count(self) -> int:
        """Get number of active connections."""
        return len(self.active_connections)

    def get_client_info(self) -> List[Dict[str, Any]]:
        """Get information about all connected clients."""
        return [
            {"client_id": client_id, "filters": self.client_filters.get(client_id, {})}
            for client_id in self.active_connections.keys()
        ]


# Global connection manager instance
manager = ConnectionManager()
