"""Tests for WebSocketClient."""
import asyncio
import json
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
import websockets
from app.services.websocket_client import WebSocketClient


pytestmark = pytest.mark.asyncio


class TestWebSocketConnection:
    """Test WebSocket connection functionality."""

    async def test_set_token(self):
        """Test setting authentication token."""
        client = WebSocketClient()
        token = "test_token_123"

        client.set_token(token)

        assert client.token == token

    async def test_connect_without_token_raises_error(self):
        """Test connecting without token raises ValueError."""
        client = WebSocketClient()

        with pytest.raises(ValueError) as exc_info:
            await client.connect()

        assert "Token not set" in str(exc_info.value)

    async def test_connect_success(self, test_token: str):
        """Test successful WebSocket connection."""
        client = WebSocketClient()
        client.set_token(test_token)

        mock_websocket = AsyncMock()

        with patch("websockets.connect", new_callable=AsyncMock) as mock_connect:
            mock_connect.return_value = mock_websocket

            with patch("asyncio.create_task") as mock_create_task:
                mock_task = MagicMock()
                mock_create_task.return_value = mock_task

                await client.connect()

                assert client.websocket == mock_websocket
                assert client.running is True
                assert client._task == mock_task
                mock_connect.assert_called_once()

    async def test_connect_timeout(self, test_token: str):
        """Test connection timeout raises TimeoutError."""
        client = WebSocketClient()
        client.set_token(test_token)

        with patch("asyncio.wait_for", side_effect=asyncio.TimeoutError):
            with pytest.raises(asyncio.TimeoutError):
                await client.connect()

    async def test_connect_failure(self, test_token: str):
        """Test connection failure raises exception."""
        client = WebSocketClient()
        client.set_token(test_token)

        with patch("websockets.connect", side_effect=Exception("Connection refused")):
            with pytest.raises(Exception) as exc_info:
                await client.connect()

            assert "Connection refused" in str(exc_info.value)

    async def test_disconnect(self, test_token: str):
        """Test disconnecting from WebSocket."""
        client = WebSocketClient()
        client.set_token(test_token)
        client.running = True

        mock_websocket = AsyncMock()

        # Create a proper async mock task
        async def mock_task_coro():
            pass

        mock_task = asyncio.create_task(mock_task_coro())

        client.websocket = mock_websocket
        client._task = mock_task

        await client.disconnect()

        assert client.running is False
        mock_websocket.close.assert_called_once()


class TestWebSocketMessages:
    """Test WebSocket message handling."""

    async def test_on_message_registers_callback(self):
        """Test registering message callback."""
        client = WebSocketClient()

        callback_called = False

        def test_callback(data: Dict[str, Any]) -> None:
            nonlocal callback_called
            callback_called = True

        client.on_message("test_type", test_callback)

        assert "test_type" in client.callbacks
        client.callbacks["test_type"]({"type": "test_type"})
        assert callback_called is True

    async def test_subscribe_without_filter(self, test_token: str):
        """Test subscribing without direction filter."""
        client = WebSocketClient()
        client.set_token(test_token)

        mock_websocket = AsyncMock()
        client.websocket = mock_websocket

        await client.subscribe()

        # Verify send was called with correct message
        mock_websocket.send.assert_called_once()
        sent_data = json.loads(mock_websocket.send.call_args[0][0])
        assert sent_data["type"] == "subscribe"
        assert "filters" not in sent_data

    async def test_subscribe_with_direction_filter(self, test_token: str):
        """Test subscribing with direction filter."""
        client = WebSocketClient()
        client.set_token(test_token)

        mock_websocket = AsyncMock()
        client.websocket = mock_websocket

        await client.subscribe(direction="IB")

        # Verify send was called with correct message
        mock_websocket.send.assert_called_once()
        sent_data = json.loads(mock_websocket.send.call_args[0][0])
        assert sent_data["type"] == "subscribe"
        assert sent_data["filters"]["direction"] == "IB"

    async def test_unsubscribe(self, test_token: str):
        """Test unsubscribing (clearing filters)."""
        client = WebSocketClient()
        client.set_token(test_token)

        mock_websocket = AsyncMock()
        client.websocket = mock_websocket

        await client.unsubscribe()

        # Verify send was called with correct message
        mock_websocket.send.assert_called_once()
        sent_data = json.loads(mock_websocket.send.call_args[0][0])
        assert sent_data["type"] == "unsubscribe"

    async def test_ping(self, test_token: str):
        """Test sending ping message."""
        client = WebSocketClient()
        client.set_token(test_token)

        mock_websocket = AsyncMock()
        client.websocket = mock_websocket

        await client.ping()

        # Verify send was called with correct message
        mock_websocket.send.assert_called_once()
        sent_data = json.loads(mock_websocket.send.call_args[0][0])
        assert sent_data["type"] == "ping"

    async def test_subscribe_without_websocket(self):
        """Test subscribe without active WebSocket connection."""
        client = WebSocketClient()
        client.websocket = None

        # Should not raise exception, just return early
        await client.subscribe()

    async def test_unsubscribe_without_websocket(self):
        """Test unsubscribe without active WebSocket connection."""
        client = WebSocketClient()
        client.websocket = None

        # Should not raise exception, just return early
        await client.unsubscribe()

    async def test_ping_without_websocket(self):
        """Test ping without active WebSocket connection."""
        client = WebSocketClient()
        client.websocket = None

        # Should not raise exception, just return early
        await client.ping()


class TestWebSocketListen:
    """Test WebSocket message listening."""

    async def test_listen_calls_registered_callback(self, test_token: str):
        """Test _listen calls registered callback for message type."""
        client = WebSocketClient()
        client.set_token(test_token)

        # Track callback calls
        callback_data = []

        def test_callback(data: Dict[str, Any]) -> None:
            callback_data.append(data)

        client.on_message("assignment_update", test_callback)

        # Mock websocket that yields one message then stops
        mock_websocket = MagicMock()
        test_message = {"type": "assignment_update", "data": {"id": 1}}

        async def mock_messages():
            yield json.dumps(test_message)

        # Make the mock websocket properly async iterable
        mock_websocket.__aiter__ = lambda self: mock_messages()
        client.websocket = mock_websocket

        # Run _listen (it will process one message then stop)
        await client._listen()

        # Verify callback was called with correct data
        assert len(callback_data) == 1
        assert callback_data[0]["type"] == "assignment_update"
        assert callback_data[0]["data"]["id"] == 1

    async def test_listen_calls_generic_callback(self, test_token: str):
        """Test _listen calls generic callback (*) for all messages."""
        client = WebSocketClient()
        client.set_token(test_token)

        # Track callback calls
        callback_data = []

        def generic_callback(data: Dict[str, Any]) -> None:
            callback_data.append(data)

        client.on_message("*", generic_callback)

        # Mock websocket that yields one message
        mock_websocket = MagicMock()
        test_message = {"type": "some_type", "data": {"value": 123}}

        async def mock_messages():
            yield json.dumps(test_message)

        # Make the mock websocket properly async iterable
        mock_websocket.__aiter__ = lambda self: mock_messages()
        client.websocket = mock_websocket

        # Run _listen
        await client._listen()

        # Verify generic callback was called
        assert len(callback_data) == 1
        assert callback_data[0]["type"] == "some_type"

    async def test_listen_handles_callback_exception(self, test_token: str):
        """Test _listen continues after callback exception."""
        client = WebSocketClient()
        client.set_token(test_token)

        def failing_callback(data: Dict[str, Any]) -> None:
            raise ValueError("Test exception")

        client.on_message("test_type", failing_callback)

        # Mock websocket
        mock_websocket = AsyncMock()
        test_message = {"type": "test_type", "data": {}}

        async def mock_messages():
            yield json.dumps(test_message)

        mock_websocket.__aiter__.return_value = mock_messages()
        client.websocket = mock_websocket

        # Should not raise exception, just log it
        await client._listen()

        # Verify running flag is set to False after listen completes
        assert client.running is False

    async def test_listen_handles_connection_closed(self, test_token: str):
        """Test _listen handles ConnectionClosed gracefully."""
        client = WebSocketClient()
        client.set_token(test_token)

        # Mock websocket that raises ConnectionClosed
        mock_websocket = AsyncMock()

        async def mock_messages():
            raise websockets.exceptions.ConnectionClosed(None, None)

        mock_websocket.__aiter__.side_effect = mock_messages

        client.websocket = mock_websocket

        # Should not raise exception
        await client._listen()

        assert client.running is False

    async def test_listen_without_websocket(self):
        """Test _listen returns early if no websocket."""
        client = WebSocketClient()
        client.websocket = None

        # Should return immediately without error
        await client._listen()


class TestWebSocketCallbacks:
    """Test callback registration and execution."""

    async def test_multiple_callbacks_registered(self):
        """Test registering multiple different callbacks."""
        client = WebSocketClient()

        callback1_called = False
        callback2_called = False

        def callback1(data: Dict[str, Any]) -> None:
            nonlocal callback1_called
            callback1_called = True

        def callback2(data: Dict[str, Any]) -> None:
            nonlocal callback2_called
            callback2_called = True

        client.on_message("type1", callback1)
        client.on_message("type2", callback2)

        assert "type1" in client.callbacks
        assert "type2" in client.callbacks

        # Call callbacks
        client.callbacks["type1"]({"type": "type1"})
        client.callbacks["type2"]({"type": "type2"})

        assert callback1_called is True
        assert callback2_called is True

    async def test_callback_override(self):
        """Test overriding existing callback."""
        client = WebSocketClient()

        call_count = 0

        def callback1(data: Dict[str, Any]) -> None:
            nonlocal call_count
            call_count += 1

        def callback2(data: Dict[str, Any]) -> None:
            nonlocal call_count
            call_count += 10

        # Register first callback
        client.on_message("test_type", callback1)
        client.callbacks["test_type"]({"type": "test_type"})
        assert call_count == 1

        # Override with second callback
        client.on_message("test_type", callback2)
        client.callbacks["test_type"]({"type": "test_type"})
        assert call_count == 11  # 1 + 10


class TestWebSocketState:
    """Test WebSocket client state management."""

    async def test_initial_state(self):
        """Test initial state of WebSocketClient."""
        client = WebSocketClient()

        assert client.base_url == "ws://localhost:8000"
        assert client.websocket is None
        assert client.token is None
        assert client.callbacks == {}
        assert client.running is False
        assert client._task is None

    async def test_custom_base_url(self):
        """Test custom base URL."""
        client = WebSocketClient(base_url="ws://example.com:9000")

        assert client.base_url == "ws://example.com:9000"

    async def test_state_after_connect(self, test_token: str):
        """Test state changes after connect."""
        client = WebSocketClient()
        client.set_token(test_token)

        mock_websocket = AsyncMock()

        with patch("websockets.connect", new_callable=AsyncMock) as mock_connect:
            mock_connect.return_value = mock_websocket

            with patch("asyncio.create_task") as mock_create_task:
                mock_task = MagicMock()
                mock_create_task.return_value = mock_task

                await client.connect()

                assert client.running is True
                assert client.websocket is not None
                assert client._task is not None

    async def test_state_after_disconnect(self, test_token: str):
        """Test state changes after disconnect."""
        client = WebSocketClient()
        client.set_token(test_token)
        client.running = True

        mock_websocket = AsyncMock()

        # Create a proper async mock task
        async def mock_task_coro():
            pass

        mock_task = asyncio.create_task(mock_task_coro())

        client.websocket = mock_websocket
        client._task = mock_task

        await client.disconnect()

        assert client.running is False
        # websocket and _task are not cleared, just closed
