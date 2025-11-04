"""Database models."""
from datetime import datetime
from enum import Enum as PyEnum
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Boolean, DateTime, Enum, ForeignKey, Integer, String, Text
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


class User(BaseModel):
    """User model."""

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
    """Ramp (loading/unloading dock) model."""

    __tablename__ = "ramps"

    code: Mapped[str] = mapped_column(String(50), unique=True, index=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Relationships
    assignments: Mapped["List[Assignment]"] = relationship("Assignment", back_populates="ramp")


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


class Assignment(BaseModel):
    """Assignment model - links ramp, load, and status."""

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
