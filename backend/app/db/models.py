"""Database models."""
from datetime import datetime
from enum import Enum as PyEnum
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Boolean, DateTime, Enum, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, BaseModel, TimestampMixin

if TYPE_CHECKING:
    from typing import List


class UserRole(str, PyEnum):
    """User role enum."""

    ADMIN = "ADMIN"
    OPERATOR = "OPERATOR"


class LoadDirection(str, PyEnum):
    """Load direction enum."""

    INBOUND = "IB"
    OUTBOUND = "OB"


class RampType(str, PyEnum):
    """Ramp type enum - Prime (gate area) or Buffer (overflow)."""

    PRIME = "PRIME"
    BUFFER = "BUFFER"


class User(BaseModel):
    """
    User model for authentication and authorization.

    Represents application users with role-based access control (RBAC).
    Admin users can manage all entities, while Operators have read-only access
    to ramps and can manage assignments.

    Attributes:
        email: Unique user email for authentication
        full_name: User's display name
        password_hash: Bcrypt hashed password
        role: ADMIN or OPERATOR role (default: OPERATOR)
        is_active: Whether user can authenticate (default: True)
        created_assignments: Assignments created by this user
        updated_assignments: Assignments last modified by this user
        audit_logs: Audit trail entries for this user's actions
    """

    __tablename__ = "users"

    email: Mapped[str] = mapped_column(String(255), unique=True, index=True, nullable=False)
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[UserRole] = mapped_column(
        Enum(UserRole, native_enum=False), default=UserRole.OPERATOR, nullable=False
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Relationships
    created_assignments: Mapped["List[Assignment]"] = relationship(
        "Assignment", back_populates="creator", foreign_keys="Assignment.created_by"
    )
    updated_assignments: Mapped["List[Assignment]"] = relationship(
        "Assignment", back_populates="updater", foreign_keys="Assignment.updated_by"
    )
    audit_logs: Mapped["List[AuditLog]"] = relationship("AuditLog", back_populates="user")


class Ramp(BaseModel):
    """
    Ramp (loading/unloading dock) model.

    Represents physical dock doors/ramps where trucks load or unload freight.
    Ramps are categorized by direction (Inbound/Outbound) and type (Prime/Buffer).
    Prime ramps are in the gate area, while Buffer ramps are overflow locations.

    Attributes:
        code: Unique ramp identifier (e.g., "R1", "R5")
        description: Human-readable ramp description
        direction: IB (Inbound) or OB (Outbound)
        type: PRIME (gate area) or BUFFER (overflow) (default: PRIME)
        assignments: Dock assignments for this ramp
    """

    __tablename__ = "ramps"

    code: Mapped[str] = mapped_column(String(50), unique=True, index=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    direction: Mapped[LoadDirection] = mapped_column(
        Enum(LoadDirection, native_enum=False), nullable=False
    )
    type: Mapped[RampType] = mapped_column(
        Enum(RampType, native_enum=False), default=RampType.PRIME, nullable=False
    )

    # Relationships
    assignments: Mapped["List[Assignment]"] = relationship("Assignment", back_populates="ramp")

    # Indexes for query optimization
    __table_args__ = (Index("ix_ramps_direction", "direction"),)


class Status(BaseModel):
    """Status model for assignments."""

    __tablename__ = "statuses"

    code: Mapped[str] = mapped_column(String(50), unique=True, index=True, nullable=False)
    label: Mapped[str] = mapped_column(String(100), nullable=False)
    color: Mapped[str] = mapped_column(String(50), nullable=False)  # hex color or name
    sort_order: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Relationships
    assignments: Mapped["List[Assignment]"] = relationship("Assignment", back_populates="status")


class Load(BaseModel):
    """Load model."""

    __tablename__ = "loads"

    reference: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    direction: Mapped[LoadDirection] = mapped_column(
        Enum(LoadDirection, native_enum=False), nullable=False
    )
    planned_arrival: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    planned_departure: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relationships
    assignments: Mapped["List[Assignment]"] = relationship("Assignment", back_populates="load")

    # Indexes for query optimization
    __table_args__ = (Index("ix_loads_direction", "direction"),)


class Assignment(BaseModel):
    """
    Assignment model - links ramp, load, and status.

    Represents the assignment of a load (truck/shipment) to a specific ramp
    with a current status. This is the core entity that tracks dock operations.
    Uses optimistic locking (version field) to prevent concurrent update conflicts.

    Attributes:
        ramp_id: Foreign key to Ramp
        load_id: Foreign key to Load
        status_id: Foreign key to Status
        eta_in: Estimated time of truck arrival
        eta_out: Estimated time of truck departure
        created_by: User ID who created the assignment
        updated_by: User ID who last updated the assignment
        ramp: Relationship to Ramp entity
        load: Relationship to Load entity
        status: Relationship to Status entity
        creator: Relationship to User who created
        updater: Relationship to User who last updated
    """

    __tablename__ = "assignments"

    ramp_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("ramps.id", ondelete="CASCADE"), nullable=False, index=True
    )
    load_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("loads.id", ondelete="CASCADE"), nullable=False, index=True
    )
    status_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("statuses.id", ondelete="RESTRICT"), nullable=False, index=True
    )
    eta_in: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    eta_out: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_by: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    updated_by: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )

    # Relationships
    ramp: Mapped["Ramp"] = relationship("Ramp", back_populates="assignments")
    load: Mapped["Load"] = relationship("Load", back_populates="assignments")
    status: Mapped["Status"] = relationship("Status", back_populates="assignments")
    creator: Mapped["User"] = relationship(
        "User", back_populates="created_assignments", foreign_keys=[created_by]
    )
    updater: Mapped["User"] = relationship(
        "User", back_populates="updated_assignments", foreign_keys=[updated_by]
    )

    # Indexes for query optimization
    __table_args__ = (
        Index("ix_assignments_created_at", "created_at"),
        Index("ix_assignments_status_ramp", "status_id", "ramp_id"),  # Composite for common queries
    )


class AuditLog(Base, TimestampMixin):
    """Audit log model."""

    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    entity_type: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    entity_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    action: Mapped[str] = mapped_column(String(50), nullable=False)  # CREATE, UPDATE, DELETE
    before_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    after_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relationships
    user: Mapped[Optional["User"]] = relationship("User", back_populates="audit_logs")
