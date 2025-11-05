"""Pytest fixtures for TUI tests."""
import asyncio
from typing import Any, Callable, Dict, List
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from app.services.api_client import APIClient, APIError
from app.services.websocket_client import WebSocketClient


# Mark all tests as asyncio by default
pytestmark = pytest.mark.asyncio


@pytest.fixture
def test_token() -> str:
    """Return a sample JWT token for testing."""
    return "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxIiwiZW1haWwiOiJhZG1pbkB0ZXN0LmNvbSIsInJvbGUiOiJBRE1JTiJ9.test"


@pytest.fixture
def test_user_data() -> Dict[str, Any]:
    """Return sample user data."""
    return {
        "id": 1,
        "email": "admin@test.com",
        "full_name": "Admin User",
        "role": "ADMIN",
        "is_active": True,
        "created_at": "2025-01-01T10:00:00",
        "updated_at": "2025-01-01T10:00:00",
        "version": 1,
    }


@pytest.fixture
def test_ramps() -> List[Dict[str, Any]]:
    """Return sample ramps data."""
    return [
        {
            "id": 1,
            "code": "R1",
            "description": "Ramp 1 Inbound",
            "direction": "IB",
            "type": "PRIME",
            "version": 1,
            "created_at": "2025-01-01T10:00:00",
            "updated_at": "2025-01-01T10:00:00",
        },
        {
            "id": 2,
            "code": "R2",
            "description": "Ramp 2 Inbound",
            "direction": "IB",
            "type": "PRIME",
            "version": 1,
            "created_at": "2025-01-01T10:00:00",
            "updated_at": "2025-01-01T10:00:00",
        },
        {
            "id": 5,
            "code": "R5",
            "description": "Ramp 5 Outbound",
            "direction": "OB",
            "type": "PRIME",
            "version": 1,
            "created_at": "2025-01-01T10:00:00",
            "updated_at": "2025-01-01T10:00:00",
        },
    ]


@pytest.fixture
def test_loads() -> List[Dict[str, Any]]:
    """Return sample loads data."""
    return [
        {
            "id": 1,
            "reference": "LOAD001",
            "direction": "IB",
            "planned_arrival": "2025-01-05T14:00:00",
            "planned_departure": None,
            "notes": "Test inbound load",
            "version": 1,
            "created_at": "2025-01-01T10:00:00",
            "updated_at": "2025-01-01T10:00:00",
        },
        {
            "id": 2,
            "reference": "LOAD002",
            "direction": "OB",
            "planned_arrival": None,
            "planned_departure": "2025-01-05T16:00:00",
            "notes": "Test outbound load",
            "version": 1,
            "created_at": "2025-01-01T10:00:00",
            "updated_at": "2025-01-01T10:00:00",
        },
    ]


@pytest.fixture
def test_statuses() -> List[Dict[str, Any]]:
    """Return sample statuses data."""
    return [
        {
            "id": 1,
            "code": "PENDING",
            "label": "Pending",
            "color": "yellow",
            "sort_order": 1,
            "version": 1,
            "created_at": "2025-01-01T10:00:00",
            "updated_at": "2025-01-01T10:00:00",
        },
        {
            "id": 2,
            "code": "ARRIVED",
            "label": "Arrived",
            "color": "blue",
            "sort_order": 2,
            "version": 1,
            "created_at": "2025-01-01T10:00:00",
            "updated_at": "2025-01-01T10:00:00",
        },
        {
            "id": 3,
            "code": "COMPLETED",
            "label": "Completed",
            "color": "green",
            "sort_order": 3,
            "version": 1,
            "created_at": "2025-01-01T10:00:00",
            "updated_at": "2025-01-01T10:00:00",
        },
    ]


@pytest.fixture
def test_assignments(
    test_ramps: List[Dict[str, Any]],
    test_loads: List[Dict[str, Any]],
    test_statuses: List[Dict[str, Any]],
    test_user_data: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Return sample assignments data."""
    return [
        {
            "id": 1,
            "ramp_id": 1,
            "load_id": 1,
            "status_id": 1,
            "eta_in": "2025-01-05T14:00:00",
            "eta_out": "2025-01-05T16:00:00",
            "created_by": 1,
            "updated_by": 1,
            "version": 1,
            "created_at": "2025-01-05T10:00:00",
            "updated_at": "2025-01-05T10:00:00",
            "ramp": test_ramps[0],
            "load": test_loads[0],
            "status": test_statuses[0],
            "creator": test_user_data,
            "updater": test_user_data,
        },
        {
            "id": 2,
            "ramp_id": 5,
            "load_id": 2,
            "status_id": 2,
            "eta_in": None,
            "eta_out": "2025-01-05T16:00:00",
            "created_by": 1,
            "updated_by": 1,
            "version": 1,
            "created_at": "2025-01-05T10:00:00",
            "updated_at": "2025-01-05T10:00:00",
            "ramp": test_ramps[2],
            "load": test_loads[1],
            "status": test_statuses[1],
            "creator": test_user_data,
            "updater": test_user_data,
        },
    ]


@pytest.fixture
def mock_api_client(
    test_token: str,
    test_user_data: Dict[str, Any],
    test_assignments: List[Dict[str, Any]],
    test_ramps: List[Dict[str, Any]],
    test_loads: List[Dict[str, Any]],
    test_statuses: List[Dict[str, Any]],
) -> APIClient:
    """Return mocked APIClient with preset responses."""
    client = APIClient()
    client.token = test_token
    client.user_data = test_user_data

    # Mock login
    async def mock_login(email: str, password: str) -> Dict[str, Any]:
        if email == "admin@test.com" and password == "admin123":
            client.token = test_token
            client.user_data = test_user_data
            return test_user_data
        raise APIError(401, "Invalid credentials")

    client.login = mock_login  # type: ignore

    # Mock get_assignments
    async def mock_get_assignments(direction: str | None = None) -> List[Dict[str, Any]]:
        if direction:
            return [a for a in test_assignments if a["ramp"]["direction"] == direction]
        return test_assignments

    client.get_assignments = mock_get_assignments  # type: ignore

    # Mock get_assignment
    async def mock_get_assignment(assignment_id: int) -> Dict[str, Any]:
        for assignment in test_assignments:
            if assignment["id"] == assignment_id:
                return assignment
        raise APIError(404, "Assignment not found")

    client.get_assignment = mock_get_assignment  # type: ignore

    # Mock create_assignment
    async def mock_create_assignment(data: Dict[str, Any]) -> Dict[str, Any]:
        new_assignment = {
            "id": 99,
            **data,
            "version": 1,
            "created_at": "2025-01-05T12:00:00",
            "updated_at": "2025-01-05T12:00:00",
            "created_by": 1,
            "updated_by": 1,
            "ramp": test_ramps[0],
            "load": test_loads[0],
            "status": test_statuses[0],
            "creator": test_user_data,
            "updater": test_user_data,
        }
        return new_assignment

    client.create_assignment = mock_create_assignment  # type: ignore

    # Mock update_assignment
    async def mock_update_assignment(
        assignment_id: int, data: Dict[str, Any]
    ) -> Dict[str, Any]:
        for assignment in test_assignments:
            if assignment["id"] == assignment_id:
                # Check version conflict
                if "version" in data and data["version"] != assignment["version"]:
                    raise APIError(409, "Version conflict")
                updated = {**assignment, **data, "version": assignment["version"] + 1}
                return updated
        raise APIError(404, "Assignment not found")

    client.update_assignment = mock_update_assignment  # type: ignore

    # Mock delete_assignment
    async def mock_delete_assignment(assignment_id: int) -> None:
        for assignment in test_assignments:
            if assignment["id"] == assignment_id:
                return
        raise APIError(404, "Assignment not found")

    client.delete_assignment = mock_delete_assignment  # type: ignore

    # Mock get_ramps
    async def mock_get_ramps() -> List[Dict[str, Any]]:
        return test_ramps

    client.get_ramps = mock_get_ramps  # type: ignore

    # Mock get_loads
    async def mock_get_loads(direction: str | None = None) -> List[Dict[str, Any]]:
        if direction:
            return [l for l in test_loads if l["direction"] == direction]
        return test_loads

    client.get_loads = mock_get_loads  # type: ignore

    # Mock get_statuses
    async def mock_get_statuses() -> List[Dict[str, Any]]:
        return test_statuses

    client.get_statuses = mock_get_statuses  # type: ignore

    # Mock get_users
    async def mock_get_users() -> List[Dict[str, Any]]:
        return [test_user_data]

    client.get_users = mock_get_users  # type: ignore

    # Mock create_load
    async def mock_create_load(data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": 99,
            **data,
            "version": 1,
            "created_at": "2025-01-05T12:00:00",
            "updated_at": "2025-01-05T12:00:00",
        }

    client.create_load = mock_create_load  # type: ignore

    # Mock create_ramp
    async def mock_create_ramp(data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": 99,
            **data,
            "version": 1,
            "created_at": "2025-01-05T12:00:00",
            "updated_at": "2025-01-05T12:00:00",
        }

    client.create_ramp = mock_create_ramp  # type: ignore

    # Mock create_user
    async def mock_create_user(data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": 99,
            **data,
            "is_active": True,
            "version": 1,
            "created_at": "2025-01-05T12:00:00",
            "updated_at": "2025-01-05T12:00:00",
        }

    client.create_user = mock_create_user  # type: ignore

    return client


@pytest.fixture
def mock_websocket_client(test_token: str) -> WebSocketClient:
    """Return mocked WebSocketClient."""
    ws_client = WebSocketClient()
    ws_client.token = test_token
    ws_client.running = False

    # Mock websocket connection
    mock_ws = MagicMock()
    mock_ws.send = AsyncMock()
    mock_ws.close = AsyncMock()
    ws_client.websocket = mock_ws

    # Mock connect
    async def mock_connect() -> None:
        ws_client.running = True

    ws_client.connect = mock_connect  # type: ignore

    # Mock subscribe
    async def mock_subscribe(direction: str | None = None) -> None:
        message = {"type": "subscribe"}
        if direction:
            message["filters"] = {"direction": direction}
        # No-op for tests

    ws_client.subscribe = mock_subscribe  # type: ignore

    # Mock unsubscribe
    async def mock_unsubscribe() -> None:
        # No-op for tests
        pass

    ws_client.unsubscribe = mock_unsubscribe  # type: ignore

    # Mock ping
    async def mock_ping() -> None:
        # No-op for tests
        pass

    ws_client.ping = mock_ping  # type: ignore

    # Mock disconnect
    async def mock_disconnect() -> None:
        ws_client.running = False

    ws_client.disconnect = mock_disconnect  # type: ignore

    return ws_client


@pytest.fixture
def mock_httpx_response():
    """Factory for creating mock httpx responses."""

    def create_response(status_code: int, json_data: Any = None, text: str = "") -> Mock:
        response = Mock()
        response.status_code = status_code
        response.json = Mock(return_value=json_data or {})
        response.text = text
        return response

    return create_response


@pytest.fixture
def event_loop():
    """Create an event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
