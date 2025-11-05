"""Tests for APIClient."""
from typing import Any, Dict, List
from unittest.mock import AsyncMock, Mock, patch

import pytest
from app.services.api_client import APIClient, APIError


pytestmark = pytest.mark.asyncio


class TestAPIClientLogin:
    """Test login functionality."""

    async def test_login_success(
        self, mock_httpx_response, test_token: str, test_user_data: Dict[str, Any]
    ):
        """Test successful login returns user data and sets token."""
        client = APIClient()

        # Mock login response
        login_response = mock_httpx_response(
            200, {"access_token": test_token, "token_type": "bearer"}
        )
        # Mock user info response
        user_response = mock_httpx_response(200, test_user_data)

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.post = AsyncMock(return_value=login_response)
            mock_context.get = AsyncMock(return_value=user_response)

            result = await client.login("admin@test.com", "admin123")

            assert result == test_user_data
            assert client.token == test_token
            assert client.user_data == test_user_data

    async def test_login_invalid_credentials(self, mock_httpx_response):
        """Test login with invalid credentials raises APIError."""
        client = APIClient()

        # Mock 401 response
        response = mock_httpx_response(401, {"detail": "Invalid credentials"})

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.post = AsyncMock(return_value=response)

            with pytest.raises(APIError) as exc_info:
                await client.login("wrong@test.com", "wrongpass")

            assert exc_info.value.status_code == 401
            assert "Invalid credentials" in exc_info.value.detail

    async def test_login_server_error(self, mock_httpx_response):
        """Test login with server error raises APIError."""
        client = APIClient()

        # Mock 500 response
        response = mock_httpx_response(500, {"detail": "Internal server error"})

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.post = AsyncMock(return_value=response)

            with pytest.raises(APIError) as exc_info:
                await client.login("admin@test.com", "admin123")

            assert exc_info.value.status_code == 500

    async def test_login_connection_error(self):
        """Test login with connection error raises exception."""
        client = APIClient()

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.post = AsyncMock(side_effect=Exception("Connection failed"))

            with pytest.raises(Exception) as exc_info:
                await client.login("admin@test.com", "admin123")

            assert "Connection failed" in str(exc_info.value)


class TestAPIClientAssignments:
    """Test assignments API methods."""

    async def test_get_assignments_all(
        self, mock_httpx_response, test_assignments: List[Dict[str, Any]]
    ):
        """Test fetching all assignments."""
        client = APIClient()
        client.token = "test_token"

        response = mock_httpx_response(200, test_assignments)

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.get = AsyncMock(return_value=response)

            result = await client.get_assignments()

            assert len(result) == 2
            assert result == test_assignments

    async def test_get_assignments_filtered_by_direction(
        self, mock_httpx_response, test_assignments: List[Dict[str, Any]]
    ):
        """Test fetching assignments filtered by direction."""
        client = APIClient()
        client.token = "test_token"

        # Filter for IB assignments
        ib_assignments = [a for a in test_assignments if a["ramp"]["direction"] == "IB"]
        response = mock_httpx_response(200, ib_assignments)

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.get = AsyncMock(return_value=response)

            result = await client.get_assignments(direction="IB")

            assert len(result) == 1
            assert result[0]["ramp"]["direction"] == "IB"

    async def test_get_assignments_not_authenticated(self):
        """Test get_assignments without token raises APIError."""
        client = APIClient()

        with pytest.raises(APIError) as exc_info:
            await client.get_assignments()

        assert exc_info.value.status_code == 401
        assert "Not authenticated" in exc_info.value.detail

    async def test_get_assignment_by_id(
        self, mock_httpx_response, test_assignments: List[Dict[str, Any]]
    ):
        """Test fetching single assignment by ID."""
        client = APIClient()
        client.token = "test_token"

        response = mock_httpx_response(200, test_assignments[0])

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.get = AsyncMock(return_value=response)

            result = await client.get_assignment(1)

            assert result["id"] == 1
            assert result == test_assignments[0]

    async def test_get_assignment_not_found(self, mock_httpx_response):
        """Test get_assignment with invalid ID raises APIError."""
        client = APIClient()
        client.token = "test_token"

        response = mock_httpx_response(404, None, "Assignment not found")

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.get = AsyncMock(return_value=response)

            with pytest.raises(APIError) as exc_info:
                await client.get_assignment(999)

            assert exc_info.value.status_code == 404

    async def test_create_assignment(self, mock_httpx_response):
        """Test creating new assignment."""
        client = APIClient()
        client.token = "test_token"

        assignment_data = {
            "ramp_id": 1,
            "load_id": 1,
            "status_id": 1,
        }

        created_assignment = {
            "id": 99,
            **assignment_data,
            "version": 1,
        }

        response = mock_httpx_response(201, created_assignment)

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.post = AsyncMock(return_value=response)

            result = await client.create_assignment(assignment_data)

            assert result["id"] == 99
            assert result["ramp_id"] == 1

    async def test_create_assignment_invalid_data(self, mock_httpx_response):
        """Test create_assignment with invalid data raises APIError."""
        client = APIClient()
        client.token = "test_token"

        response = mock_httpx_response(422, {"detail": "Validation error"})

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.post = AsyncMock(return_value=response)

            with pytest.raises(APIError) as exc_info:
                await client.create_assignment({})

            assert exc_info.value.status_code == 422

    async def test_update_assignment(self, mock_httpx_response):
        """Test updating existing assignment."""
        client = APIClient()
        client.token = "test_token"

        update_data = {"status_id": 2, "version": 1}
        updated_assignment = {"id": 1, "status_id": 2, "version": 2}

        response = mock_httpx_response(200, updated_assignment)

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.patch = AsyncMock(return_value=response)

            result = await client.update_assignment(1, update_data)

            assert result["version"] == 2
            assert result["status_id"] == 2

    async def test_update_assignment_version_conflict(self, mock_httpx_response):
        """Test update_assignment with version conflict raises APIError 409."""
        client = APIClient()
        client.token = "test_token"

        conflict_response = mock_httpx_response(
            409,
            {
                "detail": {
                    "detail": "Version conflict",
                    "current_version": 3,
                    "provided_version": 1,
                }
            },
        )

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.patch = AsyncMock(return_value=conflict_response)

            with pytest.raises(APIError) as exc_info:
                await client.update_assignment(1, {"status_id": 2, "version": 1})

            assert exc_info.value.status_code == 409
            assert "Version conflict" in exc_info.value.detail

    async def test_delete_assignment(self, mock_httpx_response):
        """Test deleting assignment."""
        client = APIClient()
        client.token = "test_token"

        response = mock_httpx_response(204)

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.delete = AsyncMock(return_value=response)

            await client.delete_assignment(1)
            # Should not raise any exception

    async def test_delete_assignment_not_found(self, mock_httpx_response):
        """Test delete_assignment with invalid ID raises APIError."""
        client = APIClient()
        client.token = "test_token"

        response = mock_httpx_response(404, None, "Assignment not found")

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.delete = AsyncMock(return_value=response)

            with pytest.raises(APIError) as exc_info:
                await client.delete_assignment(999)

            assert exc_info.value.status_code == 404


class TestAPIClientOtherEndpoints:
    """Test other API endpoints."""

    async def test_get_ramps(self, mock_httpx_response, test_ramps: List[Dict[str, Any]]):
        """Test fetching all ramps."""
        client = APIClient()
        client.token = "test_token"

        response = mock_httpx_response(200, test_ramps)

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.get = AsyncMock(return_value=response)

            result = await client.get_ramps()

            assert len(result) == 3
            assert result == test_ramps

    async def test_get_loads(self, mock_httpx_response, test_loads: List[Dict[str, Any]]):
        """Test fetching all loads."""
        client = APIClient()
        client.token = "test_token"

        response = mock_httpx_response(200, test_loads)

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.get = AsyncMock(return_value=response)

            result = await client.get_loads()

            assert len(result) == 2
            assert result == test_loads

    async def test_get_loads_filtered(
        self, mock_httpx_response, test_loads: List[Dict[str, Any]]
    ):
        """Test fetching loads filtered by direction."""
        client = APIClient()
        client.token = "test_token"

        ib_loads = [l for l in test_loads if l["direction"] == "IB"]
        response = mock_httpx_response(200, ib_loads)

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.get = AsyncMock(return_value=response)

            result = await client.get_loads(direction="IB")

            assert len(result) == 1
            assert result[0]["direction"] == "IB"

    async def test_get_statuses(
        self, mock_httpx_response, test_statuses: List[Dict[str, Any]]
    ):
        """Test fetching all statuses."""
        client = APIClient()
        client.token = "test_token"

        response = mock_httpx_response(200, test_statuses)

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.get = AsyncMock(return_value=response)

            result = await client.get_statuses()

            assert len(result) == 3
            assert result == test_statuses

    async def test_get_users(self, mock_httpx_response, test_user_data: Dict[str, Any]):
        """Test fetching all users (admin only)."""
        client = APIClient()
        client.token = "test_token"

        response = mock_httpx_response(200, [test_user_data])

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.get = AsyncMock(return_value=response)

            result = await client.get_users()

            assert len(result) == 1
            assert result[0]["email"] == "admin@test.com"

    async def test_create_load(self, mock_httpx_response):
        """Test creating new load."""
        client = APIClient()
        client.token = "test_token"

        load_data = {
            "reference": "LOAD999",
            "direction": "IB",
            "planned_arrival": "2025-01-10T10:00:00",
        }

        created_load = {
            "id": 99,
            **load_data,
            "version": 1,
        }

        response = mock_httpx_response(201, created_load)

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.post = AsyncMock(return_value=response)

            result = await client.create_load(load_data)

            assert result["id"] == 99
            assert result["reference"] == "LOAD999"

    async def test_create_ramp(self, mock_httpx_response):
        """Test creating new ramp (admin only)."""
        client = APIClient()
        client.token = "test_token"

        ramp_data = {
            "code": "R99",
            "description": "Test Ramp",
            "direction": "IB",
            "type": "PRIME",
        }

        created_ramp = {
            "id": 99,
            **ramp_data,
            "version": 1,
        }

        response = mock_httpx_response(201, created_ramp)

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.post = AsyncMock(return_value=response)

            result = await client.create_ramp(ramp_data)

            assert result["id"] == 99
            assert result["code"] == "R99"

    async def test_create_user(self, mock_httpx_response):
        """Test creating new user (admin only)."""
        client = APIClient()
        client.token = "test_token"

        user_data = {
            "email": "newuser@test.com",
            "full_name": "New User",
            "password": "password123",
            "role": "OPERATOR",
        }

        created_user = {
            "id": 99,
            "email": user_data["email"],
            "full_name": user_data["full_name"],
            "role": user_data["role"],
            "is_active": True,
            "version": 1,
        }

        response = mock_httpx_response(201, created_user)

        with patch("httpx.AsyncClient") as mock_async_client:
            mock_context = AsyncMock()
            mock_async_client.return_value.__aenter__.return_value = mock_context
            mock_context.post = AsyncMock(return_value=response)

            result = await client.create_user(user_data)

            assert result["id"] == 99
            assert result["email"] == "newuser@test.com"


class TestAPIClientErrorHandling:
    """Test error handling."""

    async def test_api_error_exception(self):
        """Test APIError exception properties."""
        error = APIError(404, "Not found")

        assert error.status_code == 404
        assert error.detail == "Not found"
        assert "404" in str(error)
        assert "Not found" in str(error)

    async def test_headers_without_token(self):
        """Test _headers raises error when not authenticated."""
        client = APIClient()

        with pytest.raises(APIError) as exc_info:
            client._headers()

        assert exc_info.value.status_code == 401
        assert "Not authenticated" in exc_info.value.detail

    async def test_headers_with_token(self):
        """Test _headers returns proper authorization header."""
        client = APIClient()
        client.token = "test_token_123"

        headers = client._headers()

        assert "Authorization" in headers
        assert headers["Authorization"] == "Bearer test_token_123"
