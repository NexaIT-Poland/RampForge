"""Ramp status aggregation helpers."""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from dateutil import parser


class RampStatus(Enum):
    """Ramp status enumeration."""

    FREE = "free"
    OCCUPIED = "occupied"
    BLOCKED = "blocked"


class RampInfo:
    """Information about a ramp and its current status."""

    EXCEPTION_CODES = {"BLOCKED", "CANCELLED", "DELAYED"}

    def __init__(self, ramp: Dict[str, Any], assignment: Optional[Dict[str, Any]] = None):
        """Initialize ramp info view-model."""
        self.ramp = ramp
        self.assignment = assignment or {}
        self.status: RampStatus = RampStatus.FREE
        self.status_label: str = "FREE"
        self.status_color: str = "green"
        self.status_code: str = "FREE"
        self.load_ref: Optional[str] = None
        # Direction comes from ramp (permanent assignment by admin), not load
        self.direction: Optional[str] = ramp.get("direction")
        self.ramp_type: Optional[str] = ramp.get("type", "PRIME")
        self.eta_in: Optional[str] = None
        self.eta_out: Optional[str] = None
        self.notes: Optional[str] = None
        self._search_blob: str = ""
        self._determine_status()

    # ---------------------------------------------------------------------#
    # Primary attributes
    # ---------------------------------------------------------------------#
    def _determine_status(self) -> None:
        """Determine ramp status based on assignment."""
        if not self.assignment:
            self.status = RampStatus.FREE
            self.status_label = "FREE"
            self.status_color = "green"
            self.status_code = "FREE"
            self.notes = None
            self._search_blob = self._build_search_blob()
            return

        status = self.assignment.get("status", {}) or {}
        status_code = status.get("code") or ""
        status_label = status.get("label") or status_code.title() or "OCCUPIED"
        self.status_code = status_code or "OCCUPIED"

        # Load metadata
        load = self.assignment.get("load", {}) or {}
        self.load_ref = load.get("reference")
        # Direction is already set from ramp in __init__, not from load
        self.notes = load.get("notes") or self.assignment.get("notes")
        self.eta_in = self.assignment.get("eta_in")
        self.eta_out = self.assignment.get("eta_out")

        if status_code in {"BLOCKED", "CANCELLED"}:
            self.status = RampStatus.BLOCKED
            self.status_label = status_label or "BLOCKED"
            self.status_color = "bright_black"
            if not self.notes:
                self.notes = load.get("notes", "Blocked")
        else:
            self.status = RampStatus.OCCUPIED
            # Check for overdue departure (for OB docks)
            if self.is_overdue and self.direction == "OB":
                self.status_label = "OVERDUE DEPARTURE"
                self.status_color = "red"
            else:
                self.status_label = status_label or "OCCUPIED"
                self.status_color = "yellow" if not self.is_overdue else "orange"
            if not self.load_ref:
                self.load_ref = "Unknown"

        self._search_blob = self._build_search_blob()

    # ---------------------------------------------------------------------#
    # Convenience helpers
    # ---------------------------------------------------------------------#
    @property
    def ramp_code(self) -> str:
        """Return ramp code."""
        return self.ramp.get("code", "???")

    @property
    def ramp_id(self) -> int:
        """Return ramp identifier."""
        return int(self.ramp.get("id", 0))

    @property
    def assignment_id(self) -> Optional[int]:
        """Return assignment identifier if any."""
        if not self.assignment:
            return None
        return self.assignment.get("id")

    @property
    def is_free(self) -> bool:
        """Return True when ramp is free."""
        return self.status == RampStatus.FREE

    @property
    def is_occupied(self) -> bool:
        """Return True when ramp is occupied."""
        return self.status == RampStatus.OCCUPIED

    @property
    def is_blocked(self) -> bool:
        """Return True when ramp is blocked."""
        return self.status == RampStatus.BLOCKED

    @property
    def zone(self) -> Optional[str]:
        """Return zone/area if defined."""
        return self.ramp.get("zone")

    @property
    def direction_label(self) -> str:
        """Return user-friendly direction label."""
        if self.direction == "IB":
            return "Inbound"
        if self.direction == "OB":
            return "Outbound"
        return "-"

    @property
    def eta_in_dt(self) -> Optional[datetime]:
        """Return parsed ETA in datetime."""
        return self._parse_datetime(self.eta_in)

    @property
    def eta_out_dt(self) -> Optional[datetime]:
        """Return parsed ETA out datetime."""
        return self._parse_datetime(self.eta_out)

    @property
    def created_at_dt(self) -> Optional[datetime]:
        """Return parsed creation datetime."""
        created = self.assignment.get("created_at") or self.assignment.get("created")
        return self._parse_datetime(created)

    @property
    def updated_at_dt(self) -> Optional[datetime]:
        """Return parsed update datetime."""
        updated = (
            self.assignment.get("updated_at")
            or self.assignment.get("updated")
            or self.assignment.get("modified_at")
        )
        return self._parse_datetime(updated)

    @property
    def last_event_user(self) -> Optional[str]:
        """Return best-effort operator name."""
        for field in ("updated_by_user", "updater", "creator"):
            user = self.assignment.get(field)
            if isinstance(user, dict):
                name = user.get("full_name") or user.get("name")
                if name:
                    return name
        return None

    @property
    def version(self) -> Optional[int]:
        """Return optimistic locking version if present."""
        return self.assignment.get("version")

    @property
    def is_overdue(self) -> bool:
        """Return True if ramp is overdue by ETA out."""
        if not self.assignment or not self.eta_out_dt or not self.is_occupied:
            return False
        eta = self.eta_out_dt
        if eta.tzinfo is None:
            eta = eta.replace(tzinfo=timezone.utc)
        return eta < datetime.now(timezone.utc)

    @property
    def is_exception(self) -> bool:
        """Return True when ramp requires attention."""
        if self.status_code in self.EXCEPTION_CODES:
            return True
        return self.is_overdue

    @property
    def search_blob(self) -> str:
        """Return lowercase blob used for search operations."""
        return self._search_blob

    def matches_query(self, query: str) -> bool:
        """Return True when ramp matches search query."""
        if not query:
            return True
        return query.lower() in self._search_blob

    # ---------------------------------------------------------------------#
    # Internal helpers
    # ---------------------------------------------------------------------#
    def _parse_datetime(self, value: Optional[str]) -> Optional[datetime]:
        """Parse ISO-8601 string to datetime."""
        if not value:
            return None
        try:
            dt = parser.isoparse(value)
        except (ValueError, TypeError):
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def _build_search_blob(self) -> str:
        """Create concatenated searchable blob."""
        parts = [
            self.ramp_code,
            self.ramp.get("description", ""),
            str(self.assignment.get("id", "")),
            self.load_ref or "",
            self.direction or "",
            self.notes or "",
            self.status_label,
        ]
        return " ".join(p for p in parts if p).lower()


def get_ramp_statuses(ramps: List[Dict[str, Any]], assignments: List[Dict[str, Any]]) -> List[RampInfo]:
    """
    Return ramp status info for all ramps.

    Args:
        ramps: Ramp payloads retrieved from backend.
        assignments: Assignment payloads retrieved from backend.
    """

    # Map ramp_id to most recent non-completed assignment.
    ramp_assignment_map: Dict[int, Dict[str, Any]] = {}
    for assignment in assignments:
        ramp_id = assignment.get("ramp_id")
        if not ramp_id:
            continue

        status_code = assignment.get("status", {}).get("code", "")
        if status_code == "COMPLETED":
            continue

        ramp_assignment_map[int(ramp_id)] = assignment

    ramp_infos: List[RampInfo] = []
    for ramp in ramps:
        ramp_id = int(ramp.get("id", 0))
        assignment = ramp_assignment_map.get(ramp_id)
        ramp_infos.append(RampInfo(ramp, assignment))

    return ramp_infos
