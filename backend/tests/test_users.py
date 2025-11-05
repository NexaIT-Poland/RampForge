"""Tests for user management endpoints."""
import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import AuditLog, User, UserRole


pytestmark = pytest.mark.asyncio


class TestGetCurrentUser:
    """Test GET /api/users/me endpoint."""

    async def test_get_current_user_admin(
        self, client: AsyncClient, admin_headers: dict[str, str], test_admin_user: User
    ):
        """Test getting current user info as admin."""
        response = await client.get("/api/users/me", headers=admin_headers)
        assert response.status_code == 200
        data = response.json()
        assert data["email"] == "admin@test.com"
        assert data["role"] == "ADMIN"
        assert data["is_active"] is True
        assert "password" not in data
        assert "password_hash" not in data

    async def test_get_current_user_operator(
        self, client: AsyncClient, operator_headers: dict[str, str], test_operator_user: User
    ):
        """Test getting current user info as operator."""
        response = await client.get("/api/users/me", headers=operator_headers)
        assert response.status_code == 200
        data = response.json()
        assert data["email"] == "operator@test.com"
        assert data["role"] == "OPERATOR"
        assert data["is_active"] is True

    async def test_get_current_user_without_auth(self, client: AsyncClient):
        """Test getting current user without authentication."""
        response = await client.get("/api/users/me")
        assert response.status_code == 403  # No credentials


class TestListUsers:
    """Test GET /api/users/ endpoint."""

    async def test_list_users_as_admin(
        self, client: AsyncClient, admin_headers: dict[str, str], test_admin_user: User, test_operator_user: User
    ):
        """Test listing users as admin."""
        response = await client.get("/api/users/", headers=admin_headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 2  # At least admin and operator
        emails = [user["email"] for user in data]
        assert "admin@test.com" in emails
        assert "operator@test.com" in emails

    async def test_list_users_as_operator(
        self, client: AsyncClient, operator_headers: dict[str, str]
    ):
        """Test listing users as operator (should be forbidden)."""
        response = await client.get("/api/users/", headers=operator_headers)
        assert response.status_code == 403
        data = response.json()
        assert "privileges" in data["detail"].lower()

    async def test_list_users_pagination(
        self, client: AsyncClient, admin_headers: dict[str, str], test_admin_user: User, test_operator_user: User
    ):
        """Test pagination on user list."""
        # Get first user
        response = await client.get("/api/users/?skip=0&limit=1", headers=admin_headers)
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1

        # Get second user
        response = await client.get("/api/users/?skip=1&limit=1", headers=admin_headers)
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1

    async def test_list_users_without_auth(self, client: AsyncClient):
        """Test listing users without authentication."""
        response = await client.get("/api/users/")
        assert response.status_code == 403


class TestCreateUser:
    """Test POST /api/users/ endpoint."""

    async def test_create_user_as_admin(
        self, client: AsyncClient, admin_headers: dict[str, str], test_db: AsyncSession
    ):
        """Test creating a new user as admin."""
        user_data = {
            "email": "newuser@test.com",
            "full_name": "New User",
            "password": "NewPassword123!",  # Valid password meeting complexity requirements
            "role": "OPERATOR",
            "is_active": True,
        }
        response = await client.post("/api/users/", json=user_data, headers=admin_headers)
        assert response.status_code == 201
        data = response.json()
        assert data["email"] == "newuser@test.com"
        assert data["full_name"] == "New User"
        assert data["role"] == "OPERATOR"
        assert data["is_active"] is True
        assert "password" not in data
        assert "password_hash" not in data
        assert "id" in data

        # Verify user was created in database
        result = await test_db.execute(
            select(User).where(User.email == "newuser@test.com")
        )
        user = result.scalar_one_or_none()
        assert user is not None
        assert user.full_name == "New User"

        # Verify audit log was created
        result = await test_db.execute(
            select(AuditLog).where(
                AuditLog.entity_type == "user",
                AuditLog.entity_id == user.id,
                AuditLog.action == "CREATE"
            )
        )
        audit = result.scalar_one_or_none()
        assert audit is not None

    async def test_create_user_duplicate_email(
        self, client: AsyncClient, admin_headers: dict[str, str], test_admin_user: User
    ):
        """Test creating user with duplicate email."""
        user_data = {
            "email": "admin@test.com",  # Already exists
            "full_name": "Duplicate Admin",
            "password": "password123",
            "role": "OPERATOR",
            "is_active": True,
        }
        response = await client.post("/api/users/", json=user_data, headers=admin_headers)
        assert response.status_code == 400
        data = response.json()
        assert "already registered" in data["detail"].lower()

    async def test_create_user_as_operator(
        self, client: AsyncClient, operator_headers: dict[str, str]
    ):
        """Test creating user as operator (should be forbidden)."""
        user_data = {
            "email": "forbidden@test.com",
            "full_name": "Forbidden User",
            "password": "password123",
            "role": "OPERATOR",
            "is_active": True,
        }
        response = await client.post("/api/users/", json=user_data, headers=operator_headers)
        assert response.status_code == 403

    async def test_create_user_invalid_email(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test creating user with invalid email."""
        user_data = {
            "email": "not-an-email",
            "full_name": "Invalid Email",
            "password": "password123",
            "role": "OPERATOR",
            "is_active": True,
        }
        response = await client.post("/api/users/", json=user_data, headers=admin_headers)
        assert response.status_code == 422  # Validation error

    async def test_create_user_missing_fields(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test creating user with missing required fields."""
        user_data = {
            "email": "incomplete@test.com",
            # Missing full_name, password, role
        }
        response = await client.post("/api/users/", json=user_data, headers=admin_headers)
        assert response.status_code == 422  # Validation error

    async def test_create_user_invalid_role(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test creating user with invalid role."""
        user_data = {
            "email": "invalidrole@test.com",
            "full_name": "Invalid Role",
            "password": "ValidPass123!",
            "role": "SUPERADMIN",  # Invalid role
            "is_active": True,
        }
        response = await client.post("/api/users/", json=user_data, headers=admin_headers)
        assert response.status_code == 422  # Validation error

    async def test_create_user_password_too_short(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test creating user with password shorter than 8 characters."""
        user_data = {
            "email": "shortpw@test.com",
            "full_name": "Short Password",
            "password": "Ab1!",  # Only 4 characters
            "role": "OPERATOR",
            "is_active": True,
        }
        response = await client.post("/api/users/", json=user_data, headers=admin_headers)
        assert response.status_code == 422  # Validation error
        data = response.json()
        assert "8 characters" in str(data["detail"]).lower()

    async def test_create_user_password_no_uppercase(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test creating user with password missing uppercase letter."""
        user_data = {
            "email": "noupper@test.com",
            "full_name": "No Uppercase",
            "password": "password123!",  # No uppercase
            "role": "OPERATOR",
            "is_active": True,
        }
        response = await client.post("/api/users/", json=user_data, headers=admin_headers)
        assert response.status_code == 422  # Validation error
        data = response.json()
        assert "uppercase" in str(data["detail"]).lower()

    async def test_create_user_password_no_lowercase(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test creating user with password missing lowercase letter."""
        user_data = {
            "email": "nolower@test.com",
            "full_name": "No Lowercase",
            "password": "PASSWORD123!",  # No lowercase
            "role": "OPERATOR",
            "is_active": True,
        }
        response = await client.post("/api/users/", json=user_data, headers=admin_headers)
        assert response.status_code == 422  # Validation error
        data = response.json()
        assert "lowercase" in str(data["detail"]).lower()

    async def test_create_user_password_no_digit(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test creating user with password missing digit."""
        user_data = {
            "email": "nodigit@test.com",
            "full_name": "No Digit",
            "password": "Password!@#",  # No digit
            "role": "OPERATOR",
            "is_active": True,
        }
        response = await client.post("/api/users/", json=user_data, headers=admin_headers)
        assert response.status_code == 422  # Validation error
        data = response.json()
        assert "digit" in str(data["detail"]).lower()

    async def test_create_user_password_no_special_char(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test creating user with password missing special character."""
        user_data = {
            "email": "nospecial@test.com",
            "full_name": "No Special",
            "password": "Password123",  # No special character
            "role": "OPERATOR",
            "is_active": True,
        }
        response = await client.post("/api/users/", json=user_data, headers=admin_headers)
        assert response.status_code == 422  # Validation error
        data = response.json()
        assert "special" in str(data["detail"]).lower()


class TestGetUser:
    """Test GET /api/users/{user_id} endpoint."""

    async def test_get_user_by_id_as_admin(
        self, client: AsyncClient, admin_headers: dict[str, str], test_operator_user: User
    ):
        """Test getting user by ID as admin."""
        response = await client.get(
            f"/api/users/{test_operator_user.id}", headers=admin_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == test_operator_user.id
        assert data["email"] == "operator@test.com"

    async def test_get_user_by_id_as_operator(
        self, client: AsyncClient, operator_headers: dict[str, str], test_admin_user: User
    ):
        """Test getting user by ID as operator (should be forbidden)."""
        response = await client.get(
            f"/api/users/{test_admin_user.id}", headers=operator_headers
        )
        assert response.status_code == 403

    async def test_get_user_not_found(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test getting non-existent user."""
        response = await client.get("/api/users/99999", headers=admin_headers)
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower()

    async def test_get_user_invalid_id(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test getting user with invalid ID format."""
        response = await client.get("/api/users/invalid", headers=admin_headers)
        assert response.status_code == 422  # Validation error


class TestUpdateUser:
    """Test PATCH /api/users/{user_id} endpoint."""

    async def test_update_user_as_admin(
        self, client: AsyncClient, admin_headers: dict[str, str], test_operator_user: User, test_db: AsyncSession
    ):
        """Test updating user as admin."""
        update_data = {
            "full_name": "Updated Operator Name",
            "is_active": False,
        }
        response = await client.patch(
            f"/api/users/{test_operator_user.id}",
            json=update_data,
            headers=admin_headers,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["full_name"] == "Updated Operator Name"
        assert data["is_active"] is False
        assert data["version"] == 2  # Version incremented

        # Verify audit log was created
        result = await test_db.execute(
            select(AuditLog).where(
                AuditLog.entity_type == "user",
                AuditLog.entity_id == test_operator_user.id,
                AuditLog.action == "UPDATE"
            )
        )
        audit = result.scalar_one_or_none()
        assert audit is not None
        assert audit.before_json is not None
        assert audit.after_json is not None

    async def test_update_user_password(
        self, client: AsyncClient, admin_headers: dict[str, str], test_operator_user: User, test_db: AsyncSession
    ):
        """Test updating user password."""
        update_data = {
            "password": "newpassword456",
        }
        response = await client.patch(
            f"/api/users/{test_operator_user.id}",
            json=update_data,
            headers=admin_headers,
        )
        assert response.status_code == 200

        # Verify password was changed by attempting login with new password
        login_response = await client.post(
            "/api/auth/login",
            json={"email": "operator@test.com", "password": "newpassword456"},
        )
        assert login_response.status_code == 200

    async def test_update_user_as_operator(
        self, client: AsyncClient, operator_headers: dict[str, str], test_admin_user: User
    ):
        """Test updating user as operator (should be forbidden)."""
        update_data = {"full_name": "Hacked Name"}
        response = await client.patch(
            f"/api/users/{test_admin_user.id}",
            json=update_data,
            headers=operator_headers,
        )
        assert response.status_code == 403

    async def test_update_user_not_found(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test updating non-existent user."""
        update_data = {"full_name": "Ghost User"}
        response = await client.patch(
            "/api/users/99999",
            json=update_data,
            headers=admin_headers,
        )
        assert response.status_code == 404

    async def test_update_user_change_role(
        self, client: AsyncClient, admin_headers: dict[str, str], test_operator_user: User
    ):
        """Test changing user role."""
        update_data = {"role": "ADMIN"}
        response = await client.patch(
            f"/api/users/{test_operator_user.id}",
            json=update_data,
            headers=admin_headers,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["role"] == "ADMIN"


class TestDeleteUser:
    """Test DELETE /api/users/{user_id} endpoint."""

    async def test_delete_user_as_admin(
        self, client: AsyncClient, admin_headers: dict[str, str], test_operator_user: User, test_db: AsyncSession
    ):
        """Test deleting user as admin."""
        user_id = test_operator_user.id
        response = await client.delete(
            f"/api/users/{user_id}", headers=admin_headers
        )
        assert response.status_code == 204

        # Verify user was deleted
        result = await test_db.execute(select(User).where(User.id == user_id))
        user = result.scalar_one_or_none()
        assert user is None

        # Verify audit log was created
        result = await test_db.execute(
            select(AuditLog).where(
                AuditLog.entity_type == "user",
                AuditLog.entity_id == user_id,
                AuditLog.action == "DELETE"
            )
        )
        audit = result.scalar_one_or_none()
        assert audit is not None

    async def test_delete_self(
        self, client: AsyncClient, admin_headers: dict[str, str], test_admin_user: User
    ):
        """Test that admin cannot delete themselves."""
        response = await client.delete(
            f"/api/users/{test_admin_user.id}", headers=admin_headers
        )
        assert response.status_code == 400
        data = response.json()
        assert "yourself" in data["detail"].lower()

    async def test_delete_user_as_operator(
        self, client: AsyncClient, operator_headers: dict[str, str], test_admin_user: User
    ):
        """Test deleting user as operator (should be forbidden)."""
        response = await client.delete(
            f"/api/users/{test_admin_user.id}", headers=operator_headers
        )
        assert response.status_code == 403

    async def test_delete_user_not_found(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test deleting non-existent user."""
        response = await client.delete("/api/users/99999", headers=admin_headers)
        assert response.status_code == 404


class TestUserPermissions:
    """Test user permission scenarios."""

    async def test_operator_cannot_access_admin_endpoints(
        self, client: AsyncClient, operator_headers: dict[str, str]
    ):
        """Test that operator cannot access any admin-only endpoints."""
        # List users
        response = await client.get("/api/users/", headers=operator_headers)
        assert response.status_code == 403

        # Create user
        response = await client.post(
            "/api/users/",
            json={
                "email": "test@test.com",
                "full_name": "Test",
                "password": "test123",
                "role": "OPERATOR",
                "is_active": True,
            },
            headers=operator_headers,
        )
        assert response.status_code == 403

        # Get user by ID
        response = await client.get("/api/users/1", headers=operator_headers)
        assert response.status_code == 403

        # Update user
        response = await client.patch(
            "/api/users/1",
            json={"full_name": "Hacked"},
            headers=operator_headers,
        )
        assert response.status_code == 403

        # Delete user
        response = await client.delete("/api/users/1", headers=operator_headers)
        assert response.status_code == 403

    async def test_operator_can_access_own_profile(
        self, client: AsyncClient, operator_headers: dict[str, str]
    ):
        """Test that operator can access their own profile."""
        response = await client.get("/api/users/me", headers=operator_headers)
        assert response.status_code == 200
        data = response.json()
        assert data["email"] == "operator@test.com"
