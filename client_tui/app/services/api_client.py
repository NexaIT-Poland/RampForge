"""API client for DCDock backend."""
from typing import Any, Dict, List, Optional

import httpx
from pydantic import BaseModel


class APIError(Exception):
    """API error exception."""

    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"API Error {status_code}: {detail}")


class APIClient:
    """HTTP client for DCDock API."""

    def __init__(self, base_url: str = "http://localhost:8000") -> None:
        """Initialize API client."""
        self.base_url = base_url
        self.token: Optional[str] = None
        self.user_data: Optional[Dict[str, Any]] = None

    async def login(self, email: str, password: str) -> Dict[str, Any]:
        """
        Login and get JWT token.

        Args:
            email: User email
            password: User password

        Returns:
            User data with token

        Raises:
            APIError: If login fails
        """
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/api/auth/login",
                json={"email": email, "password": password},
            )

            if response.status_code != 200:
                raise APIError(response.status_code, response.json().get("detail", "Login failed"))

            data = response.json()
            self.token = data["access_token"]

            # Get user info
            user_response = await client.get(
                f"{self.base_url}/api/users/me",
                headers={"Authorization": f"Bearer {self.token}"},
            )
            self.user_data = user_response.json()

            return self.user_data

    def _headers(self) -> Dict[str, str]:
        """Get authorization headers."""
        if not self.token:
            raise APIError(401, "Not authenticated")
        return {"Authorization": f"Bearer {self.token}"}

    async def get_assignments(self, direction: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all assignments."""
        params = {"direction": direction} if direction else {}
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/api/assignments/",
                headers=self._headers(),
                params=params,
            )
            if response.status_code != 200:
                raise APIError(response.status_code, response.text)
            return response.json()

    async def get_assignment(self, assignment_id: int) -> Dict[str, Any]:
        """Get assignment by ID."""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/api/assignments/{assignment_id}",
                headers=self._headers(),
            )
            if response.status_code != 200:
                raise APIError(response.status_code, response.text)
            return response.json()

    async def create_assignment(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create new assignment."""
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/api/assignments/",
                headers=self._headers(),
                json=data,
            )
            if response.status_code != 201:
                raise APIError(response.status_code, response.text)
            return response.json()

    async def update_assignment(
        self, assignment_id: int, data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update assignment."""
        async with httpx.AsyncClient() as client:
            response = await client.patch(
                f"{self.base_url}/api/assignments/{assignment_id}",
                headers=self._headers(),
                json=data,
            )
            if response.status_code == 409:
                # Conflict - return error with current data
                conflict_data = response.json()
                raise APIError(409, f"Version conflict: {conflict_data}")
            if response.status_code != 200:
                raise APIError(response.status_code, response.text)
            return response.json()

    async def delete_assignment(self, assignment_id: int) -> None:
        """Delete assignment."""
        async with httpx.AsyncClient() as client:
            response = await client.delete(
                f"{self.base_url}/api/assignments/{assignment_id}",
                headers=self._headers(),
            )
            if response.status_code != 204:
                raise APIError(response.status_code, response.text)

    async def get_ramps(self) -> List[Dict[str, Any]]:
        """Get all ramps."""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/api/ramps/",
                headers=self._headers(),
            )
            if response.status_code != 200:
                raise APIError(response.status_code, response.text)
            return response.json()

    async def get_loads(self, direction: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all loads."""
        params = {"direction": direction} if direction else {}
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/api/loads/",
                headers=self._headers(),
                params=params,
            )
            if response.status_code != 200:
                raise APIError(response.status_code, response.text)
            return response.json()

    async def get_statuses(self) -> List[Dict[str, Any]]:
        """Get all statuses."""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/api/statuses/",
                headers=self._headers(),
            )
            if response.status_code != 200:
                raise APIError(response.status_code, response.text)
            return response.json()

    async def get_users(self) -> List[Dict[str, Any]]:
        """Get all users (admin only)."""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/api/users/",
                headers=self._headers(),
            )
            if response.status_code != 200:
                raise APIError(response.status_code, response.text)
            return response.json()
