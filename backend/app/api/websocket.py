"""WebSocket API endpoint."""
from typing import Optional

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect, status
from jose import JWTError

from app.core.logging import get_logger
from app.core.security import decode_access_token
from app.ws.manager import manager

router = APIRouter(tags=["websocket"])
logger = get_logger(__name__)


async def get_websocket_user(websocket: WebSocket) -> Optional[dict]:
    """
    Authenticate WebSocket connection via token.

    Accepts token from (in order of preference):
    1. Sec-WebSocket-Protocol header: "Bearer, <JWT_TOKEN>"
    2. Query parameters (deprecated): ws://host/ws?token=JWT_TOKEN

    The Sec-WebSocket-Protocol approach is more secure as tokens
    won't appear in server logs or browser history.

    Args:
        websocket: WebSocket connection

    Returns:
        User data from token or None
    """
    token = None

    # Try to get token from Sec-WebSocket-Protocol header (preferred)
    protocols = websocket.headers.get("sec-websocket-protocol", "")
    if protocols:
        # Format: "Bearer, <token>" or just "<token>"
        parts = [p.strip() for p in protocols.split(",")]
        for part in parts:
            if part.lower().startswith("bearer."):
                # Format: "Bearer.<token>"
                token = part[7:]  # Remove "Bearer." prefix
                break
            elif len(part) > 20 and "." in part:  # JWT tokens have dots
                # Assume it's a bare token
                token = part
                break

    # Fallback to query parameter (deprecated but backward compatible)
    if not token:
        token = websocket.query_params.get("token")
        if token:
            logger.warning(
                "JWT token provided in query parameter - this is deprecated. "
                "Use Sec-WebSocket-Protocol header instead for better security."
            )

    if not token:
        return None

    payload = decode_access_token(token)
    if payload is None:
        return None

    return payload


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """
    WebSocket endpoint for real-time updates.

    Authentication (recommended - using header):
    ```python
    websockets.connect(
        "ws://localhost:8000/api/ws",
        subprotocols=["Bearer." + jwt_token]
    )
    ```

    Authentication (deprecated - using query param):
    ```
    ws://localhost:8000/api/ws?token=YOUR_JWT_TOKEN
    ```

    Message format (client to server):
    ```json
    {
        "type": "subscribe",
        "filters": {"direction": "IB"}  // Optional filters
    }
    ```

    Message format (server to client):
    ```json
    {
        "type": "assignment_updated",
        "timestamp": "2024-01-01T00:00:00",
        "assignment_id": 1,
        "action": "UPDATE",
        "user_id": 1,
        "user_email": "user@example.com",
        "data": { ... }  // Full assignment data
    }
    ```

    Supported client message types:
    - subscribe: Subscribe with optional filters
    - unsubscribe: Clear all filters
    - ping: Keep-alive ping (returns pong)

    Supported server message types:
    - connection_ack: Connection established
    - assignment_created: New assignment created
    - assignment_updated: Assignment modified
    - assignment_deleted: Assignment deleted
    - conflict_detected: Version conflict detected
    - error: Error message
    """
    # Authenticate user
    user_data = await get_websocket_user(websocket)
    if not user_data:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Authentication required")
        return

    # Generate client ID from user info
    client_id = f"user_{user_data.get('user_id')}_{id(websocket)}"

    # Connect client
    client_id = await manager.connect(websocket, client_id)

    try:
        while True:
            # Receive message from client
            message_text = await websocket.receive_text()

            # Handle message
            response = await manager.handle_client_message(client_id, message_text)

            # Send response if any
            if response:
                await websocket.send_json(response)

    except WebSocketDisconnect:
        await manager.disconnect(client_id)
    except Exception as e:
        logger.error(f"WebSocket error for client {client_id}: {e}", exc_info=True)
        await manager.disconnect(client_id)
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="Internal error")


@router.get("/ws/stats")
async def get_websocket_stats() -> dict:
    """
    Get WebSocket connection statistics.

    Returns connection count and client information.
    """
    return {
        "active_connections": manager.get_connection_count(),
        "clients": manager.get_client_info(),
    }
