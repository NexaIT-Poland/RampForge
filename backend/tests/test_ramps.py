"""Tests for ramp management endpoints."""
import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import AuditLog, Ramp, User


pytestmark = pytest.mark.asyncio


class TestListRamps:
    """Test GET /api/ramps/ endpoint."""

    async def test_list_ramps_as_admin(
        self, client: AsyncClient, admin_headers: dict[str, str], test_ramp_inbound: Ramp, test_ramp_outbound: Ramp
    ):
        """Test listing ramps as admin."""
        response = await client.get("/api/ramps/", headers=admin_headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 2  # At least the two test ramps
        codes = [ramp["code"] for ramp in data]
        assert "R1" in codes
        assert "R2" in codes

    async def test_list_ramps_as_operator(
        self, client: AsyncClient, operator_headers: dict[str, str], test_ramp_inbound: Ramp
    ):
        """Test listing ramps as operator (should be allowed)."""
        response = await client.get("/api/ramps/", headers=operator_headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 1

    async def test_list_ramps_without_auth(self, client: AsyncClient):
        """Test listing ramps without authentication."""
        response = await client.get("/api/ramps/")
        assert response.status_code == 403

    async def test_list_ramps_pagination(
        self, client: AsyncClient, admin_headers: dict[str, str], test_ramp_inbound: Ramp, test_ramp_outbound: Ramp
    ):
        """Test pagination on ramp list."""
        # Get first ramp
        response = await client.get("/api/ramps/?skip=0&limit=1", headers=admin_headers)
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1

        # Get second ramp
        response = await client.get("/api/ramps/?skip=1&limit=1", headers=admin_headers)
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1

    async def test_list_ramps_empty(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test listing ramps when none exist."""
        response = await client.get("/api/ramps/", headers=admin_headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)


class TestCreateRamp:
    """Test POST /api/ramps/ endpoint."""

    async def test_create_ramp_as_admin(
        self, client: AsyncClient, admin_headers: dict[str, str], test_db: AsyncSession
    ):
        """Test creating a new ramp as admin."""
        ramp_data = {
            "code": "R99",
            "description": "Test Ramp 99",
            "direction": "IB",
            "type": "PRIME",
        }
        response = await client.post("/api/ramps/", json=ramp_data, headers=admin_headers)
        assert response.status_code == 201
        data = response.json()
        assert data["code"] == "R99"
        assert data["description"] == "Test Ramp 99"
        assert data["direction"] == "IB"
        assert data["type"] == "PRIME"
        assert "id" in data
        assert "created_at" in data
        assert "version" in data

        # Verify ramp was created in database
        result = await test_db.execute(select(Ramp).where(Ramp.code == "R99"))
        ramp = result.scalar_one_or_none()
        assert ramp is not None
        assert ramp.description == "Test Ramp 99"

        # Verify audit log was created
        result = await test_db.execute(
            select(AuditLog).where(
                AuditLog.entity_type == "ramp",
                AuditLog.entity_id == ramp.id,
                AuditLog.action == "CREATE"
            )
        )
        audit = result.scalar_one_or_none()
        assert audit is not None

    async def test_create_ramp_outbound_buffer(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test creating an outbound buffer ramp."""
        ramp_data = {
            "code": "B1",
            "description": "Buffer Ramp 1",
            "direction": "OB",
            "type": "BUFFER",
        }
        response = await client.post("/api/ramps/", json=ramp_data, headers=admin_headers)
        assert response.status_code == 201
        data = response.json()
        assert data["code"] == "B1"
        assert data["direction"] == "OB"
        assert data["type"] == "BUFFER"

    async def test_create_ramp_without_description(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test creating ramp without optional description."""
        ramp_data = {
            "code": "R100",
            "direction": "IB",
            "type": "PRIME",
        }
        response = await client.post("/api/ramps/", json=ramp_data, headers=admin_headers)
        assert response.status_code == 201
        data = response.json()
        assert data["code"] == "R100"
        assert data["description"] is None

    async def test_create_ramp_duplicate_code(
        self, client: AsyncClient, admin_headers: dict[str, str], test_ramp_inbound: Ramp
    ):
        """Test creating ramp with duplicate code."""
        ramp_data = {
            "code": "R1",  # Already exists
            "description": "Duplicate Ramp",
            "direction": "IB",
            "type": "PRIME",
        }
        response = await client.post("/api/ramps/", json=ramp_data, headers=admin_headers)
        assert response.status_code == 400
        data = response.json()
        assert "already exists" in data["detail"].lower()

    async def test_create_ramp_as_operator(
        self, client: AsyncClient, operator_headers: dict[str, str]
    ):
        """Test creating ramp as operator (should be forbidden)."""
        ramp_data = {
            "code": "R999",
            "description": "Forbidden Ramp",
            "direction": "IB",
            "type": "PRIME",
        }
        response = await client.post("/api/ramps/", json=ramp_data, headers=operator_headers)
        assert response.status_code == 403

    async def test_create_ramp_missing_required_fields(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test creating ramp with missing required fields."""
        ramp_data = {
            "code": "R101",
            # Missing direction and type
        }
        response = await client.post("/api/ramps/", json=ramp_data, headers=admin_headers)
        assert response.status_code == 422  # Validation error

    async def test_create_ramp_invalid_direction(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test creating ramp with invalid direction."""
        ramp_data = {
            "code": "R102",
            "description": "Invalid Direction",
            "direction": "INVALID",
            "type": "PRIME",
        }
        response = await client.post("/api/ramps/", json=ramp_data, headers=admin_headers)
        assert response.status_code == 422  # Validation error

    async def test_create_ramp_invalid_type(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test creating ramp with invalid type."""
        ramp_data = {
            "code": "R103",
            "description": "Invalid Type",
            "direction": "IB",
            "type": "INVALID",
        }
        response = await client.post("/api/ramps/", json=ramp_data, headers=admin_headers)
        assert response.status_code == 422  # Validation error

    async def test_create_ramp_empty_code(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test creating ramp with empty code."""
        ramp_data = {
            "code": "",
            "description": "Empty Code",
            "direction": "IB",
            "type": "PRIME",
        }
        response = await client.post("/api/ramps/", json=ramp_data, headers=admin_headers)
        assert response.status_code == 422  # Validation error

    async def test_create_ramp_code_too_long(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test creating ramp with code exceeding max length."""
        ramp_data = {
            "code": "R" * 51,  # Max is 50
            "description": "Too Long Code",
            "direction": "IB",
            "type": "PRIME",
        }
        response = await client.post("/api/ramps/", json=ramp_data, headers=admin_headers)
        assert response.status_code == 422  # Validation error


class TestGetRamp:
    """Test GET /api/ramps/{ramp_id} endpoint."""

    async def test_get_ramp_by_id_as_admin(
        self, client: AsyncClient, admin_headers: dict[str, str], test_ramp_inbound: Ramp
    ):
        """Test getting ramp by ID as admin."""
        response = await client.get(
            f"/api/ramps/{test_ramp_inbound.id}", headers=admin_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == test_ramp_inbound.id
        assert data["code"] == "R1"

    async def test_get_ramp_by_id_as_operator(
        self, client: AsyncClient, operator_headers: dict[str, str], test_ramp_inbound: Ramp
    ):
        """Test getting ramp by ID as operator (should be allowed)."""
        response = await client.get(
            f"/api/ramps/{test_ramp_inbound.id}", headers=operator_headers
        )
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == test_ramp_inbound.id

    async def test_get_ramp_not_found(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test getting non-existent ramp."""
        response = await client.get("/api/ramps/99999", headers=admin_headers)
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower()

    async def test_get_ramp_invalid_id(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test getting ramp with invalid ID format."""
        response = await client.get("/api/ramps/invalid", headers=admin_headers)
        assert response.status_code == 422  # Validation error


class TestUpdateRamp:
    """Test PATCH /api/ramps/{ramp_id} endpoint."""

    async def test_update_ramp_as_admin(
        self, client: AsyncClient, admin_headers: dict[str, str], test_ramp_inbound: Ramp, test_db: AsyncSession
    ):
        """Test updating ramp as admin."""
        update_data = {
            "description": "Updated Ramp Description",
            "type": "BUFFER",
        }
        response = await client.patch(
            f"/api/ramps/{test_ramp_inbound.id}",
            json=update_data,
            headers=admin_headers,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["description"] == "Updated Ramp Description"
        assert data["type"] == "BUFFER"
        assert data["version"] == 2  # Version incremented

        # Verify audit log was created
        result = await test_db.execute(
            select(AuditLog).where(
                AuditLog.entity_type == "ramp",
                AuditLog.entity_id == test_ramp_inbound.id,
                AuditLog.action == "UPDATE"
            )
        )
        audit = result.scalar_one_or_none()
        assert audit is not None
        assert audit.before_json is not None
        assert audit.after_json is not None

    async def test_update_ramp_code(
        self, client: AsyncClient, admin_headers: dict[str, str], test_ramp_inbound: Ramp
    ):
        """Test updating ramp code."""
        update_data = {"code": "R1-UPDATED"}
        response = await client.patch(
            f"/api/ramps/{test_ramp_inbound.id}",
            json=update_data,
            headers=admin_headers,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["code"] == "R1-UPDATED"

    async def test_update_ramp_direction(
        self, client: AsyncClient, admin_headers: dict[str, str], test_ramp_inbound: Ramp
    ):
        """Test changing ramp direction."""
        update_data = {"direction": "OB"}
        response = await client.patch(
            f"/api/ramps/{test_ramp_inbound.id}",
            json=update_data,
            headers=admin_headers,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["direction"] == "OB"

    async def test_update_ramp_as_operator(
        self, client: AsyncClient, operator_headers: dict[str, str], test_ramp_inbound: Ramp
    ):
        """Test updating ramp as operator (should be forbidden)."""
        update_data = {"description": "Hacked Description"}
        response = await client.patch(
            f"/api/ramps/{test_ramp_inbound.id}",
            json=update_data,
            headers=operator_headers,
        )
        assert response.status_code == 403

    async def test_update_ramp_not_found(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test updating non-existent ramp."""
        update_data = {"description": "Ghost Ramp"}
        response = await client.patch(
            "/api/ramps/99999",
            json=update_data,
            headers=admin_headers,
        )
        assert response.status_code == 404

    async def test_update_ramp_partial_update(
        self, client: AsyncClient, admin_headers: dict[str, str], test_ramp_inbound: Ramp
    ):
        """Test partial update (only one field)."""
        original_code = test_ramp_inbound.code
        update_data = {"description": "Only Description Updated"}
        response = await client.patch(
            f"/api/ramps/{test_ramp_inbound.id}",
            json=update_data,
            headers=admin_headers,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["description"] == "Only Description Updated"
        assert data["code"] == original_code  # Code unchanged


class TestDeleteRamp:
    """Test DELETE /api/ramps/{ramp_id} endpoint."""

    async def test_delete_ramp_as_admin(
        self, client: AsyncClient, admin_headers: dict[str, str], test_ramp_inbound: Ramp, test_db: AsyncSession
    ):
        """Test deleting ramp as admin."""
        ramp_id = test_ramp_inbound.id
        response = await client.delete(
            f"/api/ramps/{ramp_id}", headers=admin_headers
        )
        assert response.status_code == 204

        # Verify ramp was deleted
        result = await test_db.execute(select(Ramp).where(Ramp.id == ramp_id))
        ramp = result.scalar_one_or_none()
        assert ramp is None

        # Verify audit log was created
        result = await test_db.execute(
            select(AuditLog).where(
                AuditLog.entity_type == "ramp",
                AuditLog.entity_id == ramp_id,
                AuditLog.action == "DELETE"
            )
        )
        audit = result.scalar_one_or_none()
        assert audit is not None

    async def test_delete_ramp_as_operator(
        self, client: AsyncClient, operator_headers: dict[str, str], test_ramp_inbound: Ramp
    ):
        """Test deleting ramp as operator (should be forbidden)."""
        response = await client.delete(
            f"/api/ramps/{test_ramp_inbound.id}", headers=operator_headers
        )
        assert response.status_code == 403

    async def test_delete_ramp_not_found(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test deleting non-existent ramp."""
        response = await client.delete("/api/ramps/99999", headers=admin_headers)
        assert response.status_code == 404


class TestRampPermissions:
    """Test ramp permission scenarios."""

    async def test_operator_can_read_ramps(
        self, client: AsyncClient, operator_headers: dict[str, str], test_ramp_inbound: Ramp
    ):
        """Test that operator can read ramps but not modify them."""
        # Can list ramps
        response = await client.get("/api/ramps/", headers=operator_headers)
        assert response.status_code == 200

        # Can get ramp by ID
        response = await client.get(
            f"/api/ramps/{test_ramp_inbound.id}", headers=operator_headers
        )
        assert response.status_code == 200

        # Cannot create ramp
        response = await client.post(
            "/api/ramps/",
            json={"code": "R999", "direction": "IB", "type": "PRIME"},
            headers=operator_headers,
        )
        assert response.status_code == 403

        # Cannot update ramp
        response = await client.patch(
            f"/api/ramps/{test_ramp_inbound.id}",
            json={"description": "Hacked"},
            headers=operator_headers,
        )
        assert response.status_code == 403

        # Cannot delete ramp
        response = await client.delete(
            f"/api/ramps/{test_ramp_inbound.id}", headers=operator_headers
        )
        assert response.status_code == 403


class TestRampDataIntegrity:
    """Test ramp data integrity and versioning."""

    async def test_ramp_version_increments_on_update(
        self, client: AsyncClient, admin_headers: dict[str, str], test_ramp_inbound: Ramp
    ):
        """Test that version increments on each update."""
        # First update
        response = await client.patch(
            f"/api/ramps/{test_ramp_inbound.id}",
            json={"description": "First Update"},
            headers=admin_headers,
        )
        assert response.status_code == 200
        assert response.json()["version"] == 2

        # Second update
        response = await client.patch(
            f"/api/ramps/{test_ramp_inbound.id}",
            json={"description": "Second Update"},
            headers=admin_headers,
        )
        assert response.status_code == 200
        assert response.json()["version"] == 3

    async def test_ramp_timestamps(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test that ramp has proper timestamps."""
        ramp_data = {
            "code": "R-TIMESTAMP-TEST",
            "description": "Timestamp Test",
            "direction": "IB",
            "type": "PRIME",
        }
        response = await client.post("/api/ramps/", json=ramp_data, headers=admin_headers)
        assert response.status_code == 201
        data = response.json()
        assert "created_at" in data
        assert "updated_at" in data
        assert data["created_at"] is not None
        assert data["updated_at"] is not None
