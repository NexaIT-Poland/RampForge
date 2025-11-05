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
            text("ALTER TABLE ramps ADD COLUMN direction VARCHAR(2)")
        )
        logger.info("✓ Added 'direction' column")

        # Step 2: Update existing ramps with default direction based on code pattern
        # Assuming R1-R4 are Inbound, R5+ are Outbound
        await session.execute(
            text("""
                UPDATE ramps
                SET direction = CASE
                    WHEN CAST(SUBSTR(code, 2) AS INTEGER) <= 4 THEN 'IB'
                    ELSE 'OB'
                END
                WHERE code LIKE 'R%' AND direction IS NULL
            """)
        )
        logger.info("✓ Updated existing ramps with direction based on code pattern")

        # Step 3: Set default direction for any other ramps without pattern
        await session.execute(
            text("""
                UPDATE ramps
                SET direction = 'IB'
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


async def run_migrations(session: AsyncSession) -> None:
    """Run all pending migrations."""
    logger.info("Starting database migrations...")

    migrations = [
        migrate_add_ramp_direction,
    ]

    for migration in migrations:
        try:
            await migration(session)
        except Exception as e:
            logger.error(f"Migration {migration.__name__} failed: {e}")
            raise

    logger.info("All migrations completed successfully")
