"""Seed database with initial data."""
import asyncio
from datetime import datetime, timedelta

from sqlalchemy import select

from app.core.security import get_password_hash
from app.db.migrations import run_migrations
from app.db.models import Assignment, Load, LoadDirection, Ramp, RampType, Status, User, UserRole
from app.db.session import AsyncSessionLocal, init_db


async def seed_data() -> None:
    """Seed the database with initial data."""
    print("Initializing database...")
    await init_db()

    # Run migrations first
    print("Running database migrations...")
    async with AsyncSessionLocal() as migration_session:
        await run_migrations(migration_session)

    async with AsyncSessionLocal() as db:
        # Check if data already exists
        result = await db.execute(select(User))
        if result.scalar_one_or_none() is not None:
            print("Database already seeded. Skipping...")
            return

        print("Seeding database...")

        # Create users
        users = [
            User(
                email="admin@dcdock.com",
                full_name="Admin User",
                password_hash=get_password_hash("admin123"),
                role=UserRole.ADMIN,
                is_active=True,
            ),
            User(
                email="admin2@dcdock.com",
                full_name="Admin Two",
                password_hash=get_password_hash("admin123"),
                role=UserRole.ADMIN,
                is_active=True,
            ),
            User(
                email="operator1@dcdock.com",
                full_name="John Operator",
                password_hash=get_password_hash("operator123"),
                role=UserRole.OPERATOR,
                is_active=True,
            ),
            User(
                email="operator2@dcdock.com",
                full_name="Jane Operator",
                password_hash=get_password_hash("operator123"),
                role=UserRole.OPERATOR,
                is_active=True,
            ),
            User(
                email="operator3@dcdock.com",
                full_name="Bob Operator",
                password_hash=get_password_hash("operator123"),
                role=UserRole.OPERATOR,
                is_active=True,
            ),
            User(
                email="operator4@dcdock.com",
                full_name="Alice Operator",
                password_hash=get_password_hash("operator123"),
                role=UserRole.OPERATOR,
                is_active=True,
            ),
        ]
        db.add_all(users)
        await db.flush()
        print(f"Created {len(users)} users")

        # Create ramps - each ramp is dedicated to either INBOUND or OUTBOUND
        # and classified as Prime (gate area) or Buffer (overflow area)
        ramps = [
            # Inbound Prime ramps (R1-R4) - Gate area
            Ramp(code="R1", description="Ramp 1 - Inbound Loading Bay A", direction=LoadDirection.INBOUND, type=RampType.PRIME),
            Ramp(code="R2", description="Ramp 2 - Inbound Loading Bay A", direction=LoadDirection.INBOUND, type=RampType.PRIME),
            Ramp(code="R3", description="Ramp 3 - Inbound Loading Bay B", direction=LoadDirection.INBOUND, type=RampType.PRIME),
            Ramp(code="R4", description="Ramp 4 - Inbound Loading Bay B", direction=LoadDirection.INBOUND, type=RampType.PRIME),
            # Outbound Prime ramps (R5-R8) - Gate area
            Ramp(code="R5", description="Ramp 5 - Outbound Unloading Bay C", direction=LoadDirection.OUTBOUND, type=RampType.PRIME),
            Ramp(code="R6", description="Ramp 6 - Outbound Unloading Bay C", direction=LoadDirection.OUTBOUND, type=RampType.PRIME),
            Ramp(code="R7", description="Ramp 7 - Outbound Unloading Bay D", direction=LoadDirection.OUTBOUND, type=RampType.PRIME),
            Ramp(code="R8", description="Ramp 8 - Outbound Unloading Bay D", direction=LoadDirection.OUTBOUND, type=RampType.PRIME),
        ]
        db.add_all(ramps)
        await db.flush()
        print(f"Created {len(ramps)} ramps")

        # Create statuses
        statuses = [
            Status(code="PLANNED", label="Planned", color="blue", sort_order=1),
            Status(code="ARRIVED", label="Arrived", color="cyan", sort_order=2),
            Status(code="IN_PROGRESS", label="In Progress", color="yellow", sort_order=3),
            Status(code="DELAYED", label="Delayed", color="orange", sort_order=4),
            Status(code="COMPLETED", label="Completed", color="green", sort_order=5),
            Status(code="CANCELLED", label="Cancelled", color="red", sort_order=6),
        ]
        db.add_all(statuses)
        await db.flush()
        print(f"Created {len(statuses)} statuses")

        # Get status references
        planned_status = next(s for s in statuses if s.code == "PLANNED")
        arrived_status = next(s for s in statuses if s.code == "ARRIVED")
        in_progress_status = next(s for s in statuses if s.code == "IN_PROGRESS")

        # Create loads (today's schedule)
        now = datetime.utcnow()
        loads = [
            # Inbound
            Load(
                reference="IB-2024-001",
                direction=LoadDirection.INBOUND,
                planned_arrival=now + timedelta(hours=1),
                planned_departure=now + timedelta(hours=3),
                notes="Electronics from Supplier A",
            ),
            Load(
                reference="IB-2024-002",
                direction=LoadDirection.INBOUND,
                planned_arrival=now + timedelta(hours=2),
                planned_departure=now + timedelta(hours=4),
                notes="Furniture from Supplier B",
            ),
            Load(
                reference="IB-2024-003",
                direction=LoadDirection.INBOUND,
                planned_arrival=now + timedelta(hours=3),
                planned_departure=now + timedelta(hours=5),
                notes="Textiles from Supplier C",
            ),
            Load(
                reference="IB-2024-004",
                direction=LoadDirection.INBOUND,
                planned_arrival=now + timedelta(hours=4),
                planned_departure=now + timedelta(hours=6),
                notes="Food products from Supplier D",
            ),
            # Outbound
            Load(
                reference="OB-2024-001",
                direction=LoadDirection.OUTBOUND,
                planned_arrival=now + timedelta(hours=1, minutes=30),
                planned_departure=now + timedelta(hours=3, minutes=30),
                notes="Delivery to Store 1",
            ),
            Load(
                reference="OB-2024-002",
                direction=LoadDirection.OUTBOUND,
                planned_arrival=now + timedelta(hours=2, minutes=30),
                planned_departure=now + timedelta(hours=4, minutes=30),
                notes="Delivery to Store 2",
            ),
            Load(
                reference="OB-2024-003",
                direction=LoadDirection.OUTBOUND,
                planned_arrival=now + timedelta(hours=3, minutes=30),
                planned_departure=now + timedelta(hours=5, minutes=30),
                notes="Delivery to Store 3",
            ),
            Load(
                reference="OB-2024-004",
                direction=LoadDirection.OUTBOUND,
                planned_arrival=now + timedelta(hours=5),
                planned_departure=now + timedelta(hours=7),
                notes="Export shipment to Port",
            ),
        ]
        db.add_all(loads)
        await db.flush()
        print(f"Created {len(loads)} loads")

        # Create assignments
        assignments = [
            # Inbound assignments
            Assignment(
                ramp_id=ramps[4].id,  # R5
                load_id=loads[0].id,  # IB-2024-001
                status_id=planned_status.id,
                eta_in=loads[0].planned_arrival,
                eta_out=loads[0].planned_departure,
                created_by=users[0].id,
                updated_by=users[0].id,
            ),
            Assignment(
                ramp_id=ramps[5].id,  # R6
                load_id=loads[1].id,  # IB-2024-002
                status_id=arrived_status.id,
                eta_in=loads[1].planned_arrival,
                eta_out=loads[1].planned_departure,
                created_by=users[2].id,
                updated_by=users[2].id,
            ),
            Assignment(
                ramp_id=ramps[6].id,  # R7
                load_id=loads[2].id,  # IB-2024-003
                status_id=in_progress_status.id,
                eta_in=loads[2].planned_arrival,
                eta_out=loads[2].planned_departure,
                created_by=users[3].id,
                updated_by=users[3].id,
            ),
            # Outbound assignments
            Assignment(
                ramp_id=ramps[0].id,  # R1
                load_id=loads[4].id,  # OB-2024-001
                status_id=planned_status.id,
                eta_in=loads[4].planned_arrival,
                eta_out=loads[4].planned_departure,
                created_by=users[4].id,
                updated_by=users[4].id,
            ),
            Assignment(
                ramp_id=ramps[1].id,  # R2
                load_id=loads[5].id,  # OB-2024-002
                status_id=arrived_status.id,
                eta_in=loads[5].planned_arrival,
                eta_out=loads[5].planned_departure,
                created_by=users[5].id,
                updated_by=users[5].id,
            ),
            Assignment(
                ramp_id=ramps[2].id,  # R3
                load_id=loads[6].id,  # OB-2024-003
                status_id=in_progress_status.id,
                eta_in=loads[6].planned_arrival,
                eta_out=loads[6].planned_departure,
                created_by=users[2].id,
                updated_by=users[2].id,
            ),
        ]
        db.add_all(assignments)
        await db.commit()
        print(f"Created {len(assignments)} assignments")

        print("✓ Database seeded successfully!")
        print("\nDemo credentials:")
        print("  Admin: admin@dcdock.com / admin123")
        print("  Operator: operator1@dcdock.com / operator123")


if __name__ == "__main__":
    asyncio.run(seed_data())
