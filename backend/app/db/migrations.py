"""Database migration utilities for DCDock."""
import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


async def check_column_exists(session: AsyncSession, table: str, column: str) -> bool:
    """Check if a column exists in a table (SQLite specific)."""
    try:
        result = await session.execute(text(f"PRAGMA table_info({table})"))
        columns = result.fetchall()
        return any(col[1] == column for col in columns)
    except Exception as e:
        logger.error(f"Error checking column existence: {e}")
        return False


async def migrate_add_ramp_direction(session: AsyncSession) -> None:
    """
    Add direction column to ramps table if it doesn't exist.

    Migration for adding direction field to support dedicated IB/OB docks.
    """
    logger.info("Checking if 'direction' column exists in 'ramps' table...")

    if await check_column_exists(session, "ramps", "direction"):
        logger.info("✓ Column 'direction' already exists, skipping migration")
        return

    logger.info("Adding 'direction' column to 'ramps' table...")

    try:
        # Step 1: Add the direction column as nullable (SQLite limitation)
        await session.execute(
            text("ALTER TABLE ramps ADD COLUMN direction VARCHAR(10)")
        )
        logger.info("✓ Added 'direction' column")

        # Step 2: Update existing ramps with default direction based on code pattern
        # Assuming R1-R4 are Inbound, R5+ are Outbound
        # Note: Using enum names (INBOUND/OUTBOUND) not values (IB/OB)
        await session.execute(
            text("""
                UPDATE ramps
                SET direction = CASE
                    WHEN CAST(SUBSTR(code, 2) AS INTEGER) <= 4 THEN 'INBOUND'
                    ELSE 'OUTBOUND'
                END
                WHERE code LIKE 'R%' AND direction IS NULL
            """)
        )
        logger.info("✓ Updated existing ramps with direction based on code pattern")

        # Step 3: Set default direction for any other ramps without pattern
        await session.execute(
            text("""
                UPDATE ramps
                SET direction = 'INBOUND'
                WHERE direction IS NULL
            """)
        )
        logger.info("✓ Set default direction for remaining ramps")

        await session.commit()
        logger.info("✓ Migration completed successfully: ramps.direction field added")

    except Exception as e:
        await session.rollback()
        logger.error(f"✗ Migration failed: {e}")
        raise


async def migrate_add_ramp_type(session: AsyncSession) -> None:
    """
    Add type column to ramps table if it doesn't exist.

    Migration for adding type field (Prime/Buffer) to support admin-defined dock types.
    """
    logger.info("Checking if 'type' column exists in 'ramps' table...")

    if await check_column_exists(session, "ramps", "type"):
        logger.info("✓ Column 'type' already exists, skipping migration")
        return

    logger.info("Adding 'type' column to 'ramps' table...")

    try:
        # Step 1: Add the type column as nullable
        await session.execute(
            text("ALTER TABLE ramps ADD COLUMN type VARCHAR(10)")
        )
        logger.info("✓ Added 'type' column")

        # Step 2: Update existing ramps with default type based on code pattern
        # R1-R8 are considered Prime (gate area), others Buffer
        await session.execute(
            text("""
                UPDATE ramps
                SET type = CASE
                    WHEN CAST(SUBSTR(code, 2) AS INTEGER) <= 8 THEN 'PRIME'
                    ELSE 'BUFFER'
                END
                WHERE code LIKE 'R%' AND type IS NULL
            """)
        )
        logger.info("✓ Updated existing ramps with type based on code pattern")

        # Step 3: Set default type for any other ramps without pattern
        await session.execute(
            text("""
                UPDATE ramps
                SET type = 'PRIME'
                WHERE type IS NULL
            """)
        )
        logger.info("✓ Set default type for remaining ramps")

        await session.commit()
        logger.info("✓ Migration completed successfully: ramps.type field added")

    except Exception as e:
        await session.rollback()
        logger.error(f"✗ Migration failed: {e}")
        raise


async def run_migrations(session: AsyncSession) -> None:
    """Run all pending migrations."""
    logger.info("Starting database migrations...")

    migrations = [
        migrate_add_ramp_direction,
        migrate_add_ramp_type,
    ]

    for migration in migrations:
        try:
            await migration(session)
        except Exception as e:
            logger.error(f"Migration {migration.__name__} failed: {e}")
            raise

    logger.info("All migrations completed successfully")
