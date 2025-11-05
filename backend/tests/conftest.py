"""Pytest configuration and shared fixtures."""
import asyncio
from typing import AsyncGenerator, Generator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.security import get_password_hash
from app.db.base import Base
from app.db.models import Assignment, Load, LoadDirection, Ramp, RampType, Status, User, UserRole
from app.db.session import get_db
from app.main import app


# ============================================================
# Event Loop Configuration
# ============================================================


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create event loop for async tests."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


# ============================================================
# Database Fixtures
# ============================================================


@pytest.fixture(scope="function")
async def test_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Create test database with all tables.

    Uses in-memory SQLite database that is created and destroyed for each test.
    This ensures test isolation and prevents test pollution.
    """
    # Create in-memory SQLite database with StaticPool to ensure same connection
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
        poolclass=StaticPool,  # Use StaticPool to ensure same connection
        connect_args={"check_same_thread": False},
    )

    # Create all tables
    async with engine.begin() as conn:
        # Disable foreign key constraints for easier test data creation
        await conn.execute(text("PRAGMA foreign_keys=OFF"))
        await conn.run_sync(Base.metadata.create_all)

    # Create async session factory
    async_session_factory = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )

    # Provide session
    async with async_session_factory() as session:
        yield session

    # Cleanup
    await engine.dispose()


@pytest.fixture
async def db_session(test_db: AsyncSession) -> AsyncSession:
    """Alias for test_db for convenience."""
    return test_db


# ============================================================
# FastAPI Client Fixture
# ============================================================


@pytest.fixture
async def client(test_db: AsyncSession) -> AsyncGenerator[AsyncClient, None]:
    """
    Create test HTTP client with database override.

    Overrides the get_db dependency to use test database.
    Disables rate limiting for tests.
    """
    async def override_get_db() -> AsyncGenerator[AsyncSession, None]:
        yield test_db

    app.dependency_overrides[get_db] = override_get_db

    # Disable rate limiting for tests by setting enabled=False
    from app.core.limiter import limiter
    original_enabled = limiter.enabled
    limiter.enabled = False

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    # Restore original limiter state
    limiter.enabled = original_enabled
    app.dependency_overrides.clear()


# ============================================================
# User Fixtures
# ============================================================


@pytest.fixture
async def test_admin_user(test_db: AsyncSession) -> User:
    """Create test admin user."""
    user = User(
        email="admin@test.com",
        full_name="Test Admin",
        password_hash=get_password_hash("admin123"),
        role=UserRole.ADMIN,
        is_active=True,
    )
    test_db.add(user)
    await test_db.commit()
    await test_db.refresh(user)
    return user


@pytest.fixture
async def test_operator_user(test_db: AsyncSession) -> User:
    """Create test operator user."""
    user = User(
        email="operator@test.com",
        full_name="Test Operator",
        password_hash=get_password_hash("operator123"),
        role=UserRole.OPERATOR,
        is_active=True,
    )
    test_db.add(user)
    await test_db.commit()
    await test_db.refresh(user)
    return user


@pytest.fixture
async def test_inactive_user(test_db: AsyncSession) -> User:
    """Create test inactive user."""
    user = User(
        email="inactive@test.com",
        full_name="Inactive User",
        password_hash=get_password_hash("inactive123"),
        role=UserRole.OPERATOR,
        is_active=False,
    )
    test_db.add(user)
    await test_db.commit()
    await test_db.refresh(user)
    return user


# ============================================================
# Authentication Fixtures
# ============================================================


@pytest.fixture
async def admin_token(client: AsyncClient, test_admin_user: User) -> str:
    """Get JWT token for admin user."""
    response = await client.post(
        "/api/auth/login",
        json={"email": "admin@test.com", "password": "admin123"},
    )
    assert response.status_code == 200
    return response.json()["access_token"]


@pytest.fixture
async def operator_token(client: AsyncClient, test_operator_user: User) -> str:
    """Get JWT token for operator user."""
    response = await client.post(
        "/api/auth/login",
        json={"email": "operator@test.com", "password": "operator123"},
    )
    assert response.status_code == 200
    return response.json()["access_token"]


@pytest.fixture
def admin_headers(admin_token: str) -> dict[str, str]:
    """Get authorization headers for admin user."""
    return {"Authorization": f"Bearer {admin_token}"}


@pytest.fixture
def operator_headers(operator_token: str) -> dict[str, str]:
    """Get authorization headers for operator user."""
    return {"Authorization": f"Bearer {operator_token}"}


# ============================================================
# Entity Fixtures
# ============================================================


@pytest.fixture
async def test_ramp_inbound(test_db: AsyncSession) -> Ramp:
    """Create test inbound ramp."""
    ramp = Ramp(
        code="R1",
        description="Test Inbound Ramp",
        direction=LoadDirection.INBOUND,
        type=RampType.PRIME,
    )
    test_db.add(ramp)
    await test_db.commit()
    await test_db.refresh(ramp)
    return ramp


@pytest.fixture
async def test_ramp_outbound(test_db: AsyncSession) -> Ramp:
    """Create test outbound ramp."""
    ramp = Ramp(
        code="R2",
        description="Test Outbound Ramp",
        direction=LoadDirection.OUTBOUND,
        type=RampType.PRIME,
    )
    test_db.add(ramp)
    await test_db.commit()
    await test_db.refresh(ramp)
    return ramp


@pytest.fixture
async def test_status_planned(test_db: AsyncSession) -> Status:
    """Create test 'Planned' status."""
    status = Status(
        code="PLANNED",
        label="Planned",
        color="blue",
        sort_order=1,
    )
    test_db.add(status)
    await test_db.commit()
    await test_db.refresh(status)
    return status


@pytest.fixture
async def test_status_arrived(test_db: AsyncSession) -> Status:
    """Create test 'Arrived' status."""
    status = Status(
        code="ARRIVED",
        label="Arrived",
        color="green",
        sort_order=2,
    )
    test_db.add(status)
    await test_db.commit()
    await test_db.refresh(status)
    return status


@pytest.fixture
async def test_load_inbound(test_db: AsyncSession) -> Load:
    """Create test inbound load."""
    load = Load(
        reference="IB-TEST-001",
        direction=LoadDirection.INBOUND,
        notes="Test inbound load",
    )
    test_db.add(load)
    await test_db.commit()
    await test_db.refresh(load)
    return load


@pytest.fixture
async def test_load_outbound(test_db: AsyncSession) -> Load:
    """Create test outbound load."""
    load = Load(
        reference="OB-TEST-001",
        direction=LoadDirection.OUTBOUND,
        notes="Test outbound load",
    )
    test_db.add(load)
    await test_db.commit()
    await test_db.refresh(load)
    return load


@pytest.fixture
async def test_assignment(
    test_db: AsyncSession,
    test_ramp_inbound: Ramp,
    test_load_inbound: Load,
    test_status_planned: Status,
    test_admin_user: User,
) -> Assignment:
    """Create test assignment."""
    assignment = Assignment(
        ramp_id=test_ramp_inbound.id,
        load_id=test_load_inbound.id,
        status_id=test_status_planned.id,
        created_by=test_admin_user.id,
        updated_by=test_admin_user.id,
    )
    test_db.add(assignment)
    await test_db.commit()
    await test_db.refresh(assignment)
    return assignment


# ============================================================
# Helper Fixtures
# ============================================================


@pytest.fixture
async def multiple_ramps(test_db: AsyncSession) -> list[Ramp]:
    """Create multiple test ramps."""
    ramps = [
        Ramp(code="R10", description="Ramp 10", direction=LoadDirection.INBOUND, type=RampType.PRIME),
        Ramp(code="R11", description="Ramp 11", direction=LoadDirection.OUTBOUND, type=RampType.PRIME),
        Ramp(code="R12", description="Ramp 12", direction=LoadDirection.INBOUND, type=RampType.BUFFER),
    ]
    test_db.add_all(ramps)
    await test_db.commit()
    for ramp in ramps:
        await test_db.refresh(ramp)
    return ramps


@pytest.fixture
async def multiple_statuses(test_db: AsyncSession) -> list[Status]:
    """Create multiple test statuses."""
    statuses = [
        Status(code="IN_PROGRESS", label="In Progress", color="yellow", sort_order=3),
        Status(code="COMPLETED", label="Completed", color="green", sort_order=4),
        Status(code="CANCELLED", label="Cancelled", color="red", sort_order=5),
    ]
    test_db.add_all(statuses)
    await test_db.commit()
    for status in statuses:
        await test_db.refresh(status)
    return statuses
