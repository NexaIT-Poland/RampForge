"""WebSocket client for real-time updates."""
import asyncio
import json
from typing import Any, Callable, Dict, Optional

import websockets
from websockets.client import WebSocketClientProtocol

from app.core.logging import get_logger

logger = get_logger(__name__)


class WebSocketClient:
    """
    WebSocket client for RampForge real-time updates.

    Features:
    - Automatic reconnection with exponential backoff
    - Connection status callbacks
    - Secure header-based JWT authentication
    """

    def __init__(
        self,
        base_url: str = "ws://localhost:8000",
        max_retries: int = 5,
        auto_reconnect: bool = True
    ) -> None:
        """
        Initialize WebSocket client.

        Args:
            base_url: WebSocket server URL
            max_retries: Maximum reconnection attempts (default: 5)
            auto_reconnect: Enable automatic reconnection (default: True)
        """
        self.base_url = base_url
        self.websocket: Optional[WebSocketClientProtocol] = None
        self.token: Optional[str] = None
        self.callbacks: Dict[str, Callable[[Dict[str, Any]], None]] = {}
        self.running = False
        self._task: Optional[asyncio.Task] = None

        # Reconnection settings
        self.max_retries = max_retries
        self.auto_reconnect = auto_reconnect
        self.retry_count = 0
        self.reconnecting = False
        self.on_connection_change: Optional[Callable[[bool, str], None]] = None

    def set_token(self, token: str) -> None:
        """Set authentication token."""
        self.token = token

    def set_connection_callback(self, callback: Callable[[bool, str], None]) -> None:
        """
        Set callback for connection status changes.

        Args:
            callback: Function called with (connected: bool, status: str)
                     Example statuses: "connected", "disconnected", "reconnecting"
        """
        self.on_connection_change = callback

    def on_message(self, message_type: str, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Register callback for message type."""
        self.callbacks[message_type] = callback

    async def connect(self) -> None:
        """
        Connect to WebSocket using secure header-based authentication.

        If auto_reconnect is enabled, this method will automatically retry
        connection on failure with exponential backoff.

        The token is sent via Sec-WebSocket-Protocol header instead of query
        parameters, which prevents token exposure in server logs and browser history.
        """
        if self.auto_reconnect:
            await self._connect_with_retry()
        else:
            await self._connect_once()

    async def _connect_once(self) -> None:
        """Attempt single connection without retries."""
        if not self.token:
            raise ValueError("Token not set")

        # Use secure header-based authentication (not query params)
        uri = f"{self.base_url}/api/ws"
        try:
            # Add timeout to prevent hanging
            # Send JWT token in subprotocols header for security
            self.websocket = await asyncio.wait_for(
                websockets.connect(
                    uri,
                    subprotocols=[f"Bearer.{self.token}"]
                ),
                timeout=10.0
            )
            self.running = True
            self.retry_count = 0  # Reset on successful connection
            self._task = asyncio.create_task(self._listen())

            logger.info(f"WebSocket connected to {self.base_url}")
            if self.on_connection_change:
                self.on_connection_change(True, "connected")

        except asyncio.TimeoutError:
            logger.error(f"WebSocket connection timeout to {uri}", exc_info=True)
            if self.on_connection_change:
                self.on_connection_change(False, "timeout")
            raise
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}", exc_info=True)
            if self.on_connection_change:
                self.on_connection_change(False, "failed")
            raise

    async def _connect_with_retry(self) -> None:
        """Connect with automatic retry on failure using exponential backoff."""
        self.reconnecting = True

        while self.retry_count < self.max_retries:
            try:
                await self._connect_once()
                self.reconnecting = False
                return  # Success!

            except Exception as e:
                self.retry_count += 1

                if self.retry_count >= self.max_retries:
                    logger.error(
                        f"Max reconnection attempts ({self.max_retries}) reached. "
                        "Connection failed."
                    )
                    self.reconnecting = False
                    if self.on_connection_change:
                        self.on_connection_change(False, "max_retries_reached")
                    raise

                # Exponential backoff: 2s, 4s, 8s, 16s, 32s
                backoff = min(2 ** self.retry_count, 32)
                logger.warning(
                    f"Connection failed (attempt {self.retry_count}/{self.max_retries}). "
                    f"Retrying in {backoff}s... Error: {e}"
                )

                if self.on_connection_change:
                    self.on_connection_change(False, f"reconnecting_{self.retry_count}")

                await asyncio.sleep(backoff)

    async def _listen(self) -> None:
        """Listen for WebSocket messages and handle reconnection."""
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

        except websockets.exceptions.ConnectionClosed as e:
            logger.info(f"WebSocket connection closed: {e}")
            self.running = False

            if self.on_connection_change:
                self.on_connection_change(False, "disconnected")

            # Attempt reconnection if enabled and not manually disconnected
            if self.auto_reconnect and self.token:
                logger.info("Attempting to reconnect...")
                try:
                    await self._connect_with_retry()
                except Exception as reconnect_error:
                    logger.error(f"Reconnection failed: {reconnect_error}", exc_info=True)

        except Exception as e:
            logger.error(f"WebSocket error: {e}", exc_info=True)
            self.running = False

            if self.on_connection_change:
                self.on_connection_change(False, "error")

        finally:
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
