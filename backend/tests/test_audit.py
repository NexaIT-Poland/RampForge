"""Tests for audit logging service and API."""
import json
from datetime import datetime

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import AuditLog, Ramp, User
from app.services.audit import AuditService, json_serial


# Mark all async tests with asyncio
pytestmark = pytest.mark.asyncio


class TestAuditService:
    """Test AuditService functionality."""

    @pytest.mark.asyncio
    async def test_log_action_create(
        self, test_db: AsyncSession, test_admin_user: User
    ):
        """Test logging a CREATE action."""
        entity_data = {
            "id": 1,
            "code": "R1",
            "description": "Test Ramp",
            "created_at": datetime.utcnow()
        }

        audit_log = await AuditService.log_action(
            db=test_db,
            user_id=test_admin_user.id,
            entity_type="ramp",
            entity_id=1,
            action="CREATE",
            after=entity_data,
        )

        assert audit_log.user_id == test_admin_user.id
        assert audit_log.entity_type == "ramp"
        assert audit_log.entity_id == 1
        assert audit_log.action == "CREATE"
        assert audit_log.before_json is None
        assert audit_log.after_json is not None

        # Verify JSON is valid
        after_data = json.loads(audit_log.after_json)
        assert after_data["code"] == "R1"

    async def test_log_action_update(
        self, test_db: AsyncSession, test_admin_user: User
    ):
        """Test logging an UPDATE action with before and after snapshots."""
        before_data = {
            "id": 1,
            "code": "R1",
            "description": "Old Description",
        }

        after_data = {
            "id": 1,
            "code": "R1",
            "description": "New Description",
        }

        audit_log = await AuditService.log_action(
            db=test_db,
            user_id=test_admin_user.id,
            entity_type="ramp",
            entity_id=1,
            action="UPDATE",
            before=before_data,
            after=after_data,
        )

        assert audit_log.action == "UPDATE"
        assert audit_log.before_json is not None
        assert audit_log.after_json is not None

        # Verify both snapshots are correct
        before = json.loads(audit_log.before_json)
        after = json.loads(audit_log.after_json)
        assert before["description"] == "Old Description"
        assert after["description"] == "New Description"

    async def test_log_action_delete(
        self, test_db: AsyncSession, test_admin_user: User
    ):
        """Test logging a DELETE action."""
        entity_data = {
            "id": 1,
            "code": "R1",
            "description": "Deleted Ramp",
        }

        audit_log = await AuditService.log_action(
            db=test_db,
            user_id=test_admin_user.id,
            entity_type="ramp",
            entity_id=1,
            action="DELETE",
            before=entity_data,
        )

        assert audit_log.action == "DELETE"
        assert audit_log.before_json is not None
        assert audit_log.after_json is None

        # Verify snapshot
        before = json.loads(audit_log.before_json)
        assert before["code"] == "R1"

    async def test_log_action_datetime_serialization(
        self, test_db: AsyncSession, test_admin_user: User
    ):
        """Test that datetime objects are properly serialized to JSON."""
        now = datetime.utcnow()
        entity_data = {
            "id": 1,
            "code": "R1",
            "created_at": now,
            "updated_at": now,
        }

        audit_log = await AuditService.log_action(
            db=test_db,
            user_id=test_admin_user.id,
            entity_type="ramp",
            entity_id=1,
            action="CREATE",
            after=entity_data,
        )

        assert audit_log.after_json is not None

        # Verify datetime was serialized to ISO format
        after = json.loads(audit_log.after_json)
        assert "created_at" in after
        assert isinstance(after["created_at"], str)
        assert "T" in after["created_at"]  # ISO format contains T

    async def test_log_action_without_user(
        self, test_db: AsyncSession
    ):
        """Test logging action without user (system action)."""
        audit_log = await AuditService.log_action(
            db=test_db,
            user_id=None,
            entity_type="system",
            entity_id=1,
            action="MIGRATE",
        )

        assert audit_log.user_id is None
        assert audit_log.entity_type == "system"
        assert audit_log.action == "MIGRATE"

    async def test_log_action_persisted_to_database(
        self, test_db: AsyncSession, test_admin_user: User
    ):
        """Test that audit log is actually persisted to database."""
        await AuditService.log_action(
            db=test_db,
            user_id=test_admin_user.id,
            entity_type="test",
            entity_id=999,
            action="TEST",
        )

        await test_db.commit()

        # Verify it's in the database
        result = await test_db.execute(
            select(AuditLog).where(
                AuditLog.entity_type == "test",
                AuditLog.entity_id == 999
            )
        )
        audit_log = result.scalar_one_or_none()
        assert audit_log is not None
        assert audit_log.action == "TEST"


class TestJsonSerial:
    """Test json_serial helper function."""

    def test_json_serial_datetime(self):
        """Test serializing datetime objects."""
        dt = datetime(2024, 1, 1, 12, 0, 0)
        result = json_serial(dt)
        assert isinstance(result, str)
        assert result == "2024-01-01T12:00:00"

    def test_json_serial_invalid_type(self):
        """Test that invalid types raise TypeError."""
        with pytest.raises(TypeError) as exc_info:
            json_serial(object())
        assert "not serializable" in str(exc_info.value)

    def test_json_serial_with_json_dumps(self):
        """Test that json_serial works with json.dumps."""
        data = {
            "name": "Test",
            "timestamp": datetime(2024, 1, 1, 12, 0, 0)
        }

        # This should work without errors
        json_str = json.dumps(data, default=json_serial)
        parsed = json.loads(json_str)

        assert parsed["name"] == "Test"
        assert parsed["timestamp"] == "2024-01-01T12:00:00"


class TestAuditAPI:
    """Test audit API endpoints."""

    async def test_list_audit_logs_empty(
        self, client: AsyncClient, admin_headers: dict[str, str]
    ):
        """Test listing audit logs when none exist."""
        response = await client.get("/api/audit/", headers=admin_headers)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    async def test_list_audit_logs_as_operator(
        self, client: AsyncClient, operator_headers: dict[str, str]
    ):
        """Test that operators can also view audit logs."""
        response = await client.get("/api/audit/", headers=operator_headers)
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    async def test_list_audit_logs_without_auth(
        self, client: AsyncClient
    ):
        """Test that unauthenticated requests are rejected."""
        response = await client.get("/api/audit/")
        assert response.status_code == 403

    async def test_list_audit_logs_after_create(
        self,
        client: AsyncClient,
        admin_headers: dict[str, str],
        test_ramp_inbound: Ramp,
        test_db: AsyncSession,
    ):
        """Test that audit logs appear after creating entity."""
        # Create a ramp (which logs audit)
        ramp_data = {
            "code": "AUDIT-TEST",
            "description": "Audit Test Ramp",
            "direction": "IB",
            "type": "PRIME",
        }
        create_response = await client.post(
            "/api/ramps/",
            json=ramp_data,
            headers=admin_headers
        )
        assert create_response.status_code == 201
        ramp_id = create_response.json()["id"]

        # List audit logs
        response = await client.get("/api/audit/", headers=admin_headers)
        assert response.status_code == 200
        logs = response.json()

        # Find our audit log
        our_log = None
        for log in logs:
            if log["entity_type"] == "ramp" and log["entity_id"] == ramp_id:
                our_log = log
                break

        assert our_log is not None
        assert our_log["action"] == "CREATE"
        assert our_log["entity_type"] == "ramp"

    async def test_filter_audit_logs_by_entity_type(
        self,
        client: AsyncClient,
        admin_headers: dict[str, str],
        test_db: AsyncSession,
    ):
        """Test filtering audit logs by entity_type."""
        # Create audit logs for different entity types
        await AuditService.log_action(
            db=test_db,
            user_id=1,
            entity_type="ramp",
            entity_id=1,
            action="CREATE",
        )
        await AuditService.log_action(
            db=test_db,
            user_id=1,
            entity_type="user",
            entity_id=1,
            action="CREATE",
        )
        await test_db.commit()

        # Filter by ramp
        response = await client.get(
            "/api/audit/?entity_type=ramp",
            headers=admin_headers
        )
        assert response.status_code == 200
        logs = response.json()

        # All logs should be for ramps
        for log in logs:
            assert log["entity_type"] == "ramp"

    async def test_filter_audit_logs_by_entity_id(
        self,
        client: AsyncClient,
        admin_headers: dict[str, str],
        test_db: AsyncSession,
    ):
        """Test filtering audit logs by entity_id."""
        # Create audit logs for different entities
        await AuditService.log_action(
            db=test_db,
            user_id=1,
            entity_type="ramp",
            entity_id=999,
            action="CREATE",
        )
        await AuditService.log_action(
            db=test_db,
            user_id=1,
            entity_type="ramp",
            entity_id=1000,
            action="CREATE",
        )
        await test_db.commit()

        # Filter by entity_id
        response = await client.get(
            "/api/audit/?entity_id=999",
            headers=admin_headers
        )
        assert response.status_code == 200
        logs = response.json()

        # All logs should be for entity_id 999
        for log in logs:
            assert log["entity_id"] == 999

    async def test_filter_audit_logs_by_action(
        self,
        client: AsyncClient,
        admin_headers: dict[str, str],
        test_db: AsyncSession,
    ):
        """Test filtering audit logs by action."""
        # Create audit logs for different actions
        await AuditService.log_action(
            db=test_db,
            user_id=1,
            entity_type="ramp",
            entity_id=1,
            action="CREATE",
        )
        await AuditService.log_action(
            db=test_db,
            user_id=1,
            entity_type="ramp",
            entity_id=1,
            action="UPDATE",
        )
        await AuditService.log_action(
            db=test_db,
            user_id=1,
            entity_type="ramp",
            entity_id=1,
            action="DELETE",
        )
        await test_db.commit()

        # Filter by UPDATE action
        response = await client.get(
            "/api/audit/?action=UPDATE",
            headers=admin_headers
        )
        assert response.status_code == 200
        logs = response.json()

        # All logs should be UPDATE actions
        for log in logs:
            assert log["action"] == "UPDATE"

    async def test_filter_audit_logs_combined(
        self,
        client: AsyncClient,
        admin_headers: dict[str, str],
        test_db: AsyncSession,
    ):
        """Test filtering audit logs with multiple filters."""
        # Create various audit logs
        await AuditService.log_action(
            db=test_db,
            user_id=1,
            entity_type="ramp",
            entity_id=100,
            action="UPDATE",
        )
        await AuditService.log_action(
            db=test_db,
            user_id=1,
            entity_type="ramp",
            entity_id=100,
            action="DELETE",
        )
        await AuditService.log_action(
            db=test_db,
            user_id=1,
            entity_type="user",
            entity_id=100,
            action="UPDATE",
        )
        await test_db.commit()

        # Filter by entity_type=ramp AND entity_id=100 AND action=UPDATE
        response = await client.get(
            "/api/audit/?entity_type=ramp&entity_id=100&action=UPDATE",
            headers=admin_headers
        )
        assert response.status_code == 200
        logs = response.json()

        # All logs should match all filters
        for log in logs:
            assert log["entity_type"] == "ramp"
            assert log["entity_id"] == 100
            assert log["action"] == "UPDATE"

    async def test_audit_logs_pagination(
        self,
        client: AsyncClient,
        admin_headers: dict[str, str],
        test_db: AsyncSession,
    ):
        """Test pagination on audit logs."""
        # Create multiple audit logs
        for i in range(10):
            await AuditService.log_action(
                db=test_db,
                user_id=1,
                entity_type="test",
                entity_id=i,
                action="CREATE",
            )
        await test_db.commit()

        # Get first 5
        response = await client.get(
            "/api/audit/?entity_type=test&skip=0&limit=5",
            headers=admin_headers
        )
        assert response.status_code == 200
        logs = response.json()
        assert len(logs) == 5

        # Get next 5
        response = await client.get(
            "/api/audit/?entity_type=test&skip=5&limit=5",
            headers=admin_headers
        )
        assert response.status_code == 200
        logs = response.json()
        assert len(logs) == 5

    async def test_audit_logs_ordered_by_created_at_desc(
        self,
        client: AsyncClient,
        admin_headers: dict[str, str],
        test_db: AsyncSession,
    ):
        """Test that audit logs are ordered by created_at descending (newest first)."""
        # Create multiple audit logs
        for i in range(5):
            await AuditService.log_action(
                db=test_db,
                user_id=1,
                entity_type="order_test",
                entity_id=i,
                action="CREATE",
            )
        await test_db.commit()

        # Get logs
        response = await client.get(
            "/api/audit/?entity_type=order_test",
            headers=admin_headers
        )
        assert response.status_code == 200
        logs = response.json()

        # Verify order (newest first)
        if len(logs) >= 2:
            # First log should be newer or equal to second log
            first_created = datetime.fromisoformat(logs[0]["created_at"].replace("Z", "+00:00"))
            second_created = datetime.fromisoformat(logs[1]["created_at"].replace("Z", "+00:00"))
            assert first_created >= second_created

    async def test_audit_log_response_structure(
        self,
        client: AsyncClient,
        admin_headers: dict[str, str],
        test_db: AsyncSession,
    ):
        """Test that audit log response has correct structure."""
        # Create an audit log
        await AuditService.log_action(
            db=test_db,
            user_id=1,
            entity_type="structure_test",
            entity_id=1,
            action="CREATE",
            after={"key": "value"},
        )
        await test_db.commit()

        # Get logs
        response = await client.get(
            "/api/audit/?entity_type=structure_test",
            headers=admin_headers
        )
        assert response.status_code == 200
        logs = response.json()
        assert len(logs) >= 1

        log = logs[0]
        # Verify required fields
        assert "id" in log
        assert "user_id" in log
        assert "entity_type" in log
        assert "entity_id" in log
        assert "action" in log
        assert "created_at" in log
        # before_json and after_json are in the response
        assert "before_json" in log
        assert "after_json" in log
        # Verify the after_json contains our data
        assert log["after_json"] == '{"key": "value"}'
