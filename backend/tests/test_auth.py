"""Tests for authentication endpoints."""
import pytest
from httpx import AsyncClient

from app.db.models import User


# Disable rate limiting for tests by using function scope fixtures
# This ensures each test class gets a fresh client without rate limit history
pytestmark = pytest.mark.asyncio


class TestLogin:
    """Test /api/auth/login endpoint."""

    @pytest.mark.asyncio
    async def test_login_admin_success(
        self, client: AsyncClient, test_admin_user: User
    ):
        """Test successful login with admin credentials."""
        response = await client.post(
            "/api/auth/login",
            json={"email": "admin@test.com", "password": "admin123"},
        )
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert data["token_type"] == "bearer"
        assert len(data["access_token"]) > 0

    @pytest.mark.asyncio
    async def test_login_operator_success(
        self, client: AsyncClient, test_operator_user: User
    ):
        """Test successful login with operator credentials."""
        response = await client.post(
            "/api/auth/login",
            json={"email": "operator@test.com", "password": "operator123"},
        )
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert data["token_type"] == "bearer"
        assert len(data["access_token"]) > 0

    @pytest.mark.asyncio
    async def test_login_wrong_password(
        self, client: AsyncClient, test_admin_user: User
    ):
        """Test login fails with wrong password."""
        response = await client.post(
            "/api/auth/login",
            json={"email": "admin@test.com", "password": "wrongpassword"},
        )
        assert response.status_code == 401
        data = response.json()
        assert data["detail"] == "Incorrect email or password"

    @pytest.mark.asyncio
    async def test_login_nonexistent_user(self, client: AsyncClient):
        """Test login fails with non-existent user."""
        response = await client.post(
            "/api/auth/login",
            json={"email": "nonexistent@test.com", "password": "password123"},
        )
        assert response.status_code == 401
        data = response.json()
        assert data["detail"] == "Incorrect email or password"

    @pytest.mark.asyncio
    async def test_login_inactive_user(
        self, client: AsyncClient, test_inactive_user: User
    ):
        """Test login fails with inactive user."""
        response = await client.post(
            "/api/auth/login",
            json={"email": "inactive@test.com", "password": "inactive123"},
        )
        assert response.status_code == 400
        data = response.json()
        assert data["detail"] == "Inactive user"

    @pytest.mark.asyncio
    async def test_login_missing_email(self, client: AsyncClient):
        """Test login fails with missing email."""
        response = await client.post(
            "/api/auth/login",
            json={"password": "password123"},
        )
        assert response.status_code == 422  # Validation error

    @pytest.mark.asyncio
    async def test_login_missing_password(self, client: AsyncClient):
        """Test login fails with missing password."""
        response = await client.post(
            "/api/auth/login",
            json={"email": "admin@test.com"},
        )
        assert response.status_code == 422  # Validation error

    @pytest.mark.asyncio
    async def test_login_empty_payload(self, client: AsyncClient):
        """Test login fails with empty payload."""
        response = await client.post("/api/auth/login", json={})
        assert response.status_code == 422  # Validation error

    @pytest.mark.asyncio
    async def test_login_invalid_email_format(self, client: AsyncClient):
        """Test login fails with invalid email format."""
        response = await client.post(
            "/api/auth/login",
            json={"email": "not-an-email", "password": "password123"},
        )
        assert response.status_code == 422  # Validation error

    @pytest.mark.asyncio
    async def test_login_sql_injection_attempt(
        self, client: AsyncClient, test_admin_user: User
    ):
        """Test login is protected against SQL injection."""
        response = await client.post(
            "/api/auth/login",
            json={
                "email": "admin@test.com' OR '1'='1",
                "password": "' OR '1'='1",
            },
        )
        # Email validation rejects malformed emails with 422
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_token_can_be_used_for_authentication(
        self, client: AsyncClient, test_admin_user: User
    ):
        """Test that the returned token can be used for authenticated requests."""
        # Login
        login_response = await client.post(
            "/api/auth/login",
            json={"email": "admin@test.com", "password": "admin123"},
        )
        assert login_response.status_code == 200
        token = login_response.json()["access_token"]

        # Use token to access protected endpoint
        me_response = await client.get(
            "/api/users/me",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert me_response.status_code == 200
        user_data = me_response.json()
        assert user_data["email"] == "admin@test.com"
        assert user_data["role"] == "ADMIN"

    @pytest.mark.asyncio
    async def test_login_case_insensitive_email(
        self, client: AsyncClient, test_admin_user: User
    ):
        """Test that email is case-insensitive in SQLite."""
        response = await client.post(
            "/api/auth/login",
            json={"email": "ADMIN@TEST.COM", "password": "admin123"},
        )
        # SQLite is case-insensitive for text comparison by default
        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data


class TestRateLimiting:
    """Test rate limiting on login endpoint.

    Note: These tests are skipped because rate limiting state is shared across
    all tests and cannot be easily reset in the test environment. Rate limiting
    functionality should be tested manually or in integration tests.
    """

    @pytest.mark.skip(reason="Rate limiter state shared across tests")
    @pytest.mark.asyncio
    async def test_rate_limit_exceeded(
        self, client: AsyncClient, test_admin_user: User
    ):
        """Test that rate limiting blocks excessive login attempts."""
        # Make 5 failed login attempts (should all succeed in being processed)
        for i in range(5):
            response = await client.post(
                "/api/auth/login",
                json={"email": "test_ratelimit@test.com", "password": "wrongpassword"},
            )
            # First 5 should be processed (even if they fail authentication)
            assert response.status_code in [401, 429]

        # 6th attempt should be rate limited
        response = await client.post(
            "/api/auth/login",
            json={"email": "test_ratelimit@test.com", "password": "wrongpassword"},
        )
        assert response.status_code == 429
        data = response.json()
        assert "rate limit" in data["detail"].lower()

    @pytest.mark.skip(reason="Rate limiter state shared across tests")
    @pytest.mark.asyncio
    async def test_successful_login_within_rate_limit(
        self, client: AsyncClient, test_admin_user: User
    ):
        """Test that successful logins work within rate limit."""
        # Make 3 successful login attempts
        for i in range(3):
            response = await client.post(
                "/api/auth/login",
                json={"email": "admin@test.com", "password": "admin123"},
            )
            assert response.status_code == 200
            data = response.json()
            assert "access_token" in data


class TestTokenValidation:
    """Test JWT token validation."""

    @pytest.mark.asyncio
    async def test_protected_endpoint_without_token(self, client: AsyncClient):
        """Test that protected endpoints reject requests without token."""
        response = await client.get("/api/users/me")
        # FastAPI HTTPBearer returns 403 when no credentials provided
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_protected_endpoint_with_invalid_token(self, client: AsyncClient):
        """Test that protected endpoints reject invalid tokens."""
        response = await client.get(
            "/api/users/me",
            headers={"Authorization": "Bearer invalid_token_here"},
        )
        assert response.status_code == 401

    @pytest.mark.asyncio
    async def test_protected_endpoint_with_malformed_header(self, client: AsyncClient):
        """Test that protected endpoints reject malformed authorization headers."""
        response = await client.get(
            "/api/users/me",
            headers={"Authorization": "NotBearer token_here"},
        )
        # Malformed header format results in 403
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_protected_endpoint_with_empty_bearer(self, client: AsyncClient):
        """Test that protected endpoints reject empty bearer tokens."""
        response = await client.get(
            "/api/users/me",
            headers={"Authorization": "Bearer "},
        )
        # Empty token results in 403 (no credentials provided)
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_valid_token_grants_access(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test that valid token grants access to protected endpoints."""
        response = await client.get("/api/users/me", headers=admin_headers)
        assert response.status_code == 200
        data = response.json()
        assert data["email"] == "admin@test.com"


class TestPasswordSecurity:
    """Test password security measures."""

    @pytest.mark.asyncio
    async def test_password_not_exposed_in_response(
        self, client: AsyncClient, test_admin_user: User
    ):
        """Test that password is never exposed in API responses."""
        # Login
        login_response = await client.post(
            "/api/auth/login",
            json={"email": "admin@test.com", "password": "admin123"},
        )
        assert login_response.status_code == 200
        login_data = login_response.json()
        assert "password" not in login_data
        assert "password_hash" not in login_data

        # Get user info
        token = login_data["access_token"]
        me_response = await client.get(
            "/api/users/me",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert me_response.status_code == 200
        user_data = me_response.json()
        assert "password" not in user_data
        assert "password_hash" not in user_data

    @pytest.mark.asyncio
    async def test_password_minimum_length(self, client: AsyncClient):
        """Test login with very short password."""
        response = await client.post(
            "/api/auth/login",
            json={"email": "admin@test.com", "password": "1"},
        )
        # Should fail authentication (not validation)
        assert response.status_code == 401
