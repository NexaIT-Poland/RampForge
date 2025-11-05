"""WebSocket client for real-time updates."""
import asyncio
import json
from typing import Any, Callable, Dict, Optional

import websockets
from websockets.client import WebSocketClientProtocol

from app.core.logging import get_logger

logger = get_logger(__name__)


class WebSocketClient:
    """WebSocket client for DCDock real-time updates."""

    def __init__(self, base_url: str = "ws://localhost:8000") -> None:
        """Initialize WebSocket client."""
        self.base_url = base_url
        self.websocket: Optional[WebSocketClientProtocol] = None
        self.token: Optional[str] = None
        self.callbacks: Dict[str, Callable[[Dict[str, Any]], None]] = {}
        self.running = False
        self._task: Optional[asyncio.Task] = None

    def set_token(self, token: str) -> None:
        """Set authentication token."""
        self.token = token

    def on_message(self, message_type: str, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Register callback for message type."""
        self.callbacks[message_type] = callback

    async def connect(self) -> None:
        """Connect to WebSocket."""
        if not self.token:
            raise ValueError("Token not set")

        uri = f"{self.base_url}/api/ws?token={self.token}"
        try:
            # Add timeout to prevent hanging
            self.websocket = await asyncio.wait_for(
                websockets.connect(uri),
                timeout=10.0
            )
            self.running = True
            self._task = asyncio.create_task(self._listen())
            logger.info(f"WebSocket connected to {self.base_url}")
        except asyncio.TimeoutError:
            logger.error(f"WebSocket connection timeout to {uri}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}", exc_info=True)
            raise

    async def _listen(self) -> None:
        """Listen for WebSocket messages."""
        if not self.websocket:
            return

        try:
            async for message in self.websocket:
                data = json.loads(message)
                message_type = data.get("type")

                # Call registered callback
                if message_type in self.callbacks:
                    try:
                        self.callbacks[message_type](data)
                    except Exception as e:
                        logger.error(f"Error in callback for {message_type}: {e}", exc_info=True)

                # Call generic callback if registered
                if "*" in self.callbacks:
                    try:
                        self.callbacks["*"](data)
                    except Exception as e:
                        logger.error(f"Error in generic callback: {e}", exc_info=True)

        except websockets.exceptions.ConnectionClosed:
            logger.info("WebSocket connection closed")
        except Exception as e:
            logger.error(f"WebSocket error: {e}", exc_info=True)
        finally:
            self.running = False
            logger.debug("WebSocket listener stopped")

    async def subscribe(self, direction: Optional[str] = None) -> None:
        """Subscribe with optional direction filter."""
        if not self.websocket:
            return

        message = {"type": "subscribe"}
        if direction:
            message["filters"] = {"direction": direction}

        await self.websocket.send(json.dumps(message))

    async def unsubscribe(self) -> None:
        """Clear all filters."""
        if not self.websocket:
            return

        await self.websocket.send(json.dumps({"type": "unsubscribe"}))

    async def ping(self) -> None:
        """Send ping to keep connection alive."""
        if not self.websocket:
            return

        await self.websocket.send(json.dumps({"type": "ping"}))

    async def disconnect(self) -> None:
        """Disconnect from WebSocket."""
        self.running = False
        if self.websocket:
            await self.websocket.close()
        if self._task:
            await self._task
