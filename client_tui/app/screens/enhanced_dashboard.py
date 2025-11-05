"""Enhanced dock dashboard with full functionality for operators and admins."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical, Container
from textual.screen import Screen, ModalScreen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    Select,
    Static,
)

from app.services import APIClient, WebSocketClient
from app.services.ramp_status import RampInfo, get_ramp_statuses

logger = logging.getLogger(__name__)


# ============================================================================
# MODALS
# ============================================================================


class OccupyDockModal(ModalScreen[Dict[str, Any]]):
    """Modal for occupying a dock with load information."""

    DEFAULT_CSS = """
    OccupyDockModal {
        align: center middle;
    }

    #modal-container {
        width: 60;
        height: auto;
        background: $surface;
        border: thick $primary;
        padding: 1 2;
    }

    .modal-title {
        width: 100%;
        text-align: center;
        text-style: bold;
        color: $primary;
        margin-bottom: 1;
    }

    .input-group {
        width: 100%;
        height: auto;
        margin-bottom: 1;
    }

    .input-label {
        width: 100%;
        color: $text;
        margin-bottom: 0;
    }

    #button-bar {
        width: 100%;
        height: 3;
        layout: horizontal;
        align: center middle;
    }
    """

    def __init__(self, dock_code: str, direction: str) -> None:
        super().__init__()
        self.dock_code = dock_code
        self.direction = direction
        self.is_outbound = direction == "OB"

    def compose(self) -> ComposeResult:
        with Container(id="modal-container"):
            dept_label = "Outbound" if self.is_outbound else "Inbound"
            yield Static(f"Occupy Dock {self.dock_code} ({dept_label})", classes="modal-title")

            with Vertical(classes="input-group"):
                yield Static("Load Reference:", classes="input-label")
                yield Input(placeholder="Enter load reference (e.g., IB-12345)", id="load-ref")

            # Only show departure date for outbound docks
            if self.is_outbound:
                with Vertical(classes="input-group"):
                    yield Static("Planned Departure (YYYY-MM-DD HH:MM):", classes="input-label")
                    yield Input(placeholder="2024-12-01 14:00", id="departure-date")

            with Vertical(classes="input-group"):
                yield Static("Notes (optional):", classes="input-label")
                yield Input(placeholder="Additional notes...", id="notes")

            with Horizontal(id="button-bar"):
                yield Button("Confirm", variant="primary", id="confirm")
                yield Button("Cancel", id="cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "confirm":
            load_ref = self.query_one("#load-ref", Input).value
            notes = self.query_one("#notes", Input).value

            if not load_ref:
                return  # Validation

            result = {
                "load_ref": load_ref,
                "direction": self.direction,
                "notes": notes,
            }

            # Add departure date for outbound docks
            if self.is_outbound:
                departure_date = self.query_one("#departure-date", Input).value
                if departure_date:
                    result["departure_date"] = departure_date

            self.dismiss(result)
        else:
            self.dismiss(None)


class BlockDockModal(ModalScreen[Dict[str, Any]]):
    """Modal for blocking a dock with reason."""

    DEFAULT_CSS = """
    BlockDockModal {
        align: center middle;
    }

    #modal-container {
        width: 60;
        height: auto;
        background: $surface;
        border: thick $error;
        padding: 1 2;
    }

    .modal-title {
        width: 100%;
        text-align: center;
        text-style: bold;
        color: $error;
        margin-bottom: 1;
    }

    .input-group {
        width: 100%;
        height: auto;
        margin-bottom: 1;
    }

    .input-label {
        width: 100%;
        color: $text;
        margin-bottom: 0;
    }

    #reason-input {
        width: 100%;
        height: 5;
    }

    #button-bar {
        width: 100%;
        height: 3;
        layout: horizontal;
        align: center middle;
    }
    """

    def __init__(self, dock_code: str) -> None:
        super().__init__()
        self.dock_code = dock_code

    def compose(self) -> ComposeResult:
        with Container(id="modal-container"):
            yield Static(f"⚠️  Block Dock {self.dock_code}", classes="modal-title")

            with Vertical(classes="input-group"):
                yield Static("Reason for blocking (required):", classes="input-label")
                yield Input(
                    placeholder="e.g., Maintenance, Damaged dock, Safety issue...",
                    id="reason-input"
                )

            with Horizontal(id="button-bar"):
                yield Button("Block Dock", variant="error", id="confirm")
                yield Button("Cancel", id="cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "confirm":
            reason = self.query_one("#reason-input", Input).value

            if not reason:
                return  # Validation

            self.dismiss({"reason": reason})
        else:
            self.dismiss(None)


class AddDockModal(ModalScreen[Dict[str, Any]]):
    """Modal for adding a new dock (admin only)."""

    DEFAULT_CSS = """
    AddDockModal {
        align: center middle;
    }

    #modal-container {
        width: 60;
        height: auto;
        background: $surface;
        border: thick $success;
        padding: 1 2;
    }

    .modal-title {
        width: 100%;
        text-align: center;
        text-style: bold;
        color: $success;
        margin-bottom: 1;
    }

    .input-group {
        width: 100%;
        height: auto;
        margin-bottom: 1;
    }

    .input-label {
        width: 100%;
        color: $text;
        margin-bottom: 0;
    }

    #button-bar {
        width: 100%;
        height: 3;
        layout: horizontal;
        align: center middle;
    }
    """

    def compose(self) -> ComposeResult:
        with Container(id="modal-container"):
            yield Static("➕ Add New Dock", classes="modal-title")

            with Vertical(classes="input-group"):
                yield Static("Dock Code (e.g., R9, R10):", classes="input-label")
                yield Input(placeholder="R9", id="code")

            with Vertical(classes="input-group"):
                yield Static("Department (permanent assignment):", classes="input-label")
                yield Select([("Inbound (IB)", "IB"), ("Outbound (OB)", "OB")], id="direction")

            with Vertical(classes="input-group"):
                yield Static("Type:", classes="input-label")
                yield Select([("Prime (Gate Area)", "PRIME"), ("Buffer (Overflow)", "BUFFER")], id="dock-type")

            with Vertical(classes="input-group"):
                yield Static("Description (optional):", classes="input-label")
                yield Input(placeholder="Dock description...", id="description")

            with Horizontal(id="button-bar"):
                yield Button("Add Dock", variant="success", id="confirm")
                yield Button("Cancel", id="cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "confirm":
            code = self.query_one("#code", Input).value
            direction = self.query_one("#direction", Select).value
            dock_type = self.query_one("#dock-type", Select).value
            description = self.query_one("#description", Input).value

            if not code:
                return

            self.dismiss({
                "code": code,
                "direction": direction or "IB",
                "type": dock_type or "PRIME",
                "description": description,
            })
        else:
            self.dismiss(None)


class AddUserModal(ModalScreen[Dict[str, Any]]):
    """Modal for adding a new user (admin only)."""

    DEFAULT_CSS = """
    AddUserModal {
        align: center middle;
    }

    #modal-container {
        width: 60;
        height: auto;
        background: $surface;
        border: thick $success;
        padding: 1 2;
    }

    .modal-title {
        width: 100%;
        text-align: center;
        text-style: bold;
        color: $success;
        margin-bottom: 1;
    }

    .input-group {
        width: 100%;
        height: auto;
        margin-bottom: 1;
    }

    .input-label {
        width: 100%;
        color: $text;
        margin-bottom: 0;
    }

    #button-bar {
        width: 100%;
        height: 3;
        layout: horizontal;
        align: center middle;
    }
    """

    def compose(self) -> ComposeResult:
        with Container(id="modal-container"):
            yield Static("👤 Add New User", classes="modal-title")

            with Vertical(classes="input-group"):
                yield Static("Email:", classes="input-label")
                yield Input(placeholder="user@dcdock.com", id="email")

            with Vertical(classes="input-group"):
                yield Static("Full Name:", classes="input-label")
                yield Input(placeholder="John Doe", id="fullname")

            with Vertical(classes="input-group"):
                yield Static("Password:", classes="input-label")
                yield Input(placeholder="Password", password=True, id="password")

            with Vertical(classes="input-group"):
                yield Static("Role:", classes="input-label")
                yield Select([("Operator", "OPERATOR"), ("Admin", "ADMIN")], id="role")

            with Horizontal(id="button-bar"):
                yield Button("Add User", variant="success", id="confirm")
                yield Button("Cancel", id="cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "confirm":
            email = self.query_one("#email", Input).value
            fullname = self.query_one("#fullname", Input).value
            password = self.query_one("#password", Input).value
            role = self.query_one("#role", Select).value

            if not email or not fullname or not password:
                return

            self.dismiss({
                "email": email,
                "full_name": fullname,
                "password": password,
                "role": role or "OPERATOR",
            })
        else:
            self.dismiss(None)


# ============================================================================
# INFO PANEL
# ============================================================================


class InfoPanel(Static):
    """Right-side information panel showing dock statistics."""

    DEFAULT_CSS = """
    InfoPanel {
        width: 25;
        height: 100%;
        background: $surface-darken-2;
        border-left: solid $panel;
        padding: 1;
        overflow-y: auto;
    }

    InfoPanel .panel-title {
        text-style: bold;
        color: $primary;
        margin-bottom: 1;
    }

    InfoPanel .info-section {
        margin-bottom: 1;
        padding: 1;
        background: $surface-darken-1;
        border: solid $panel-darken-1;
    }
    """

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self._content: Optional[Static] = None

    def compose(self) -> ComposeResult:
        yield Static("📊 Stats", classes="panel-title")
        self._content = Static("Loading...", classes="info-section")
        yield self._content

    def update_stats(
        self,
        total: int = 0,
        prime_free: int = 0,
        prime_occupied: int = 0,
        buffer_free: int = 0,
        buffer_occupied: int = 0,
        ib_count: int = 0,
        ob_count: int = 0,
        urgent: int = 0,
        blocked: int = 0,
    ) -> None:
        """Update statistics display."""
        if not self._content:
            return

        lines = [
            f"[white]Total:[/] {total}",
            "",
            "[yellow]PRIME[/]",
            f"🟢 {prime_free}  🔵 {prime_occupied}",
            "",
            "[magenta]BUFFER[/]",
            f"🟢 {buffer_free}  🔵 {buffer_occupied}",
            "",
            f"📥 IB: {ib_count}",
            f"📤 OB: {ob_count}",
            "",
            f"[red]🔴 {urgent}[/]",
            f"[orange_red1]⚠️  {blocked}[/]",
        ]

        self._content.update("\n".join(lines))


# ============================================================================
# MAIN DASHBOARD
# ============================================================================


class EnhancedDockDashboard(Screen):
    """Enhanced dashboard with full operator and admin functionality."""

    CSS = """
    EnhancedDockDashboard {
        layout: vertical;
    }

    Header {
        background: $boost;
    }

    #header-bar {
        width: 100%;
        height: 3;
        background: $boost;
        padding: 0 2;
        layout: horizontal;
        align: center middle;
    }

    #user-info {
        width: 1fr;
        text-align: left;
        color: $text;
    }

    #header-title {
        width: auto;
        text-align: right;
        color: $text-muted;
    }

    #action-bar {
        width: 100%;
        height: 3;
        padding: 0 2;
        layout: horizontal;
        align: center middle;
        background: $panel;
    }

    #action-bar Button {
        margin: 0 1;
    }

    #filter-bar {
        width: 100%;
        height: 3;
        padding: 0 2;
        layout: horizontal;
        align: center middle;
        background: $surface;
        border-bottom: solid $boost;
    }

    #search-input {
        width: 1fr;
        margin: 0 1;
    }

    #status-bar {
        width: 100%;
        height: 1;
        background: $panel;
        padding: 0 2;
        color: $text;
    }

    #main-container {
        width: 100%;
        height: 1fr;
        layout: horizontal;
    }

    #table-container {
        width: 1fr;
        height: 100%;
        padding: 1;
        background: $surface-darken-1;
        overflow-y: auto;
    }

    #prime-section {
        width: 100%;
        margin-bottom: 2;
    }

    #buffer-section {
        width: 100%;
        margin-bottom: 2;
    }

    .section-title {
        width: 100%;
        height: 2;
        background: $boost;
        color: $text;
        text-style: bold;
        padding: 0 1;
        margin-bottom: 1;
    }

    #prime-table {
        width: 100%;
        height: auto;
        margin-bottom: 2;
    }

    #buffer-table {
        width: 100%;
        height: auto;
    }

    DataTable {
        background: $surface-darken-2;
        border: solid $panel-darken-1;
        height: auto;
    }

    DataTable .datatable--header {
        text-style: bold;
    }

    Footer {
        background: $boost;
    }
    """

    BINDINGS = [
        ("r", "refresh", "Refresh"),
        ("escape", "quit", "Quit"),
        ("ctrl+f", "focus_search", "Search"),
        ("1", "filter_all", "All"),
        ("2", "filter_ib", "IB"),
        ("3", "filter_ob", "OB"),
        ("o", "occupy_dock", "Occupy"),
        ("f", "free_dock", "Free"),
        ("b", "block_dock", "Block"),
    ]

    def __init__(
        self,
        api_client: APIClient,
        ws_client: WebSocketClient,
        user_data: Dict[str, Any],
    ) -> None:
        super().__init__()
        self.api_client = api_client
        self.ws_client = ws_client
        self.user_data = user_data
        self.is_admin = user_data.get("role") == "ADMIN"
        self.ramps: List[Dict[str, Any]] = []
        self.assignments: List[Dict[str, Any]] = []
        self.ramp_infos: List[RampInfo] = []
        self.search_query: str = ""
        self.direction_filter: Optional[str] = None
        self.selected_dock: Optional[RampInfo] = None

    def compose(self) -> ComposeResult:
        """Compose enhanced dashboard layout."""
        yield Header()

        # Header bar
        with Horizontal(id="header-bar"):
            role_badge = "🔑 ADMIN" if self.is_admin else "👤 OPERATOR"
            yield Label(
                f"{role_badge} {self.user_data.get('full_name', 'User')}",
                id="user-info",
            )
            yield Label("🚀 DCDock Operations", id="header-title")

        # Action bar (buttons)
        with Horizontal(id="action-bar"):
            yield Button("🔄 Refresh", id="btn-refresh", variant="default")
            yield Button("➕ Occupy Dock", id="btn-occupy", variant="primary")
            yield Button("🟢 Free Dock", id="btn-free", variant="success")
            yield Button("🔴 Block Dock", id="btn-block", variant="error")

            if self.is_admin:
                yield Button("➕ Add Dock", id="btn-add-dock", variant="warning")
                yield Button("👤 Add User", id="btn-add-user", variant="warning")

        # Filter bar
        with Horizontal(id="filter-bar"):
            yield Label("🔍")
            yield Input(placeholder="Search dock, load, notes...", id="search-input")
            yield Label("[1]All [2]IB [3]OB")

        yield Label("🔄 Initializing...", id="status-bar")

        # Main content: Tables (left) + Info Panel (right)
        with Horizontal(id="main-container"):
            with Container(id="table-container"):
                # Prime section
                with Vertical(id="prime-section"):
                    yield Static("🏭 PRIME DOCKS (Gate Area)", classes="section-title")
                    yield DataTable(id="prime-table", cursor_type="row")

                # Buffer section
                with Vertical(id="buffer-section"):
                    yield Static("📦 BUFFER DOCKS (Overflow Area)", classes="section-title")
                    yield DataTable(id="buffer-table", cursor_type="row")

            yield InfoPanel()

        yield Footer()

    async def on_mount(self) -> None:
        """Initialize dashboard and connect WebSocket."""
        logger.info("EnhancedDockDashboard mount started")

        # Setup tables
        prime_table = self.query_one("#prime-table", DataTable)
        buffer_table = self.query_one("#buffer-table", DataTable)

        for table in [prime_table, buffer_table]:
            table.add_columns(
                "Dock",
                "Status",
                "Direction",
                "Load Ref",
                "ETA Out",
                "Duration",
                "Priority",
                "Notes",
            )

        # Connect WebSocket
        try:
            await self.ws_client.connect()
            self.ws_client.on_message("assignment_created", self._handle_ws_event)
            self.ws_client.on_message("assignment_updated", self._handle_ws_event)
            self.ws_client.on_message("assignment_deleted", self._handle_ws_event)
            self._update_status("🔗 Connected")
        except Exception as exc:
            logger.exception("WebSocket connection failed")
            self._update_status(f"⚠️ Offline – {exc}")

        await self.action_refresh()
        logger.info("EnhancedDockDashboard mount completed")

    async def on_unmount(self) -> None:
        """Clean up WebSocket subscriptions."""
        try:
            await self.ws_client.disconnect()
        except Exception:
            logger.exception("Failed to disconnect WebSocket")

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button clicks."""
        button_id = event.button.id

        if button_id == "btn-refresh":
            await self.action_refresh()
        elif button_id == "btn-occupy":
            self.action_occupy_dock()
        elif button_id == "btn-free":
            self.action_free_dock()
        elif button_id == "btn-block":
            self.action_block_dock()
        elif button_id == "btn-add-dock" and self.is_admin:
            self._add_dock()
        elif button_id == "btn-add-user" and self.is_admin:
            self._add_user()

    async def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        """Handle row selection."""
        if not event.row_key:
            return

        # Find selected dock
        for info in self.ramp_infos:
            if str(info.ramp_id) == str(event.row_key.value):
                self.selected_dock = info
                break

    async def action_refresh(self) -> None:
        """Reload all data from API."""
        logger.info("Refreshing dashboard data")
        try:
            self.ramps = await self.api_client.get_ramps()
            self.assignments = await self.api_client.get_assignments()
        except Exception as exc:
            logger.exception("Failed to load data")
            self._update_status(f"❌ Error: {exc}")
            return

        self.ramp_infos = get_ramp_statuses(self.ramps, self.assignments)
        self._update_tables()
        self._update_info_panel()
        self._update_status(f"✓ Loaded {len(self.ramp_infos)} docks")

    def action_occupy_dock(self) -> None:
        """Occupy selected dock with load."""
        if not self.selected_dock:
            self._update_status("⚠️ Select a dock first")
            return

        if not self.selected_dock.is_free:
            self._update_status("⚠️ Dock is already occupied")
            return

        def handle_result(result: Optional[Dict[str, Any]]) -> None:
            if result:
                self.run_worker(self._occupy_dock_async(result), exclusive=True)

        # Pass direction from dock (permanent assignment by admin)
        self.app.push_screen(
            OccupyDockModal(self.selected_dock.ramp_code, self.selected_dock.direction),
            callback=handle_result
        )

    async def _occupy_dock_async(self, modal_data: Dict[str, Any]) -> None:
        """Async worker to occupy dock via API."""
        try:
            # Get statuses to find IN_PROGRESS status ID
            statuses = await self.api_client.get_statuses()
            in_progress_status = next((s for s in statuses if s.get("code") == "IN_PROGRESS"), None)

            if not in_progress_status:
                self._update_status("❌ Error: IN_PROGRESS status not found")
                return

            # Create the load
            load_data = {
                "reference": modal_data["load_ref"],
                "direction": modal_data["direction"],
                "notes": modal_data.get("notes", ""),
            }

            # Handle departure date for outbound docks
            if "departure_date" in modal_data and modal_data["departure_date"]:
                try:
                    # Parse departure date (format: "YYYY-MM-DD HH:MM")
                    departure_str = modal_data["departure_date"].strip()
                    departure_dt = datetime.strptime(departure_str, "%Y-%m-%d %H:%M")
                    # Convert to ISO format for API
                    load_data["planned_departure"] = departure_dt.isoformat()
                except ValueError as e:
                    self._update_status(f"❌ Invalid departure date format: {e}")
                    return

            load = await self.api_client.create_load(load_data)
            logger.info(f"Created load: {load}")

            # Create the assignment
            assignment_data = {
                "ramp_id": self.selected_dock.ramp_id,
                "load_id": load["id"],
                "status_id": in_progress_status["id"],
            }

            # Add eta_out from load's planned_departure if available
            if "planned_departure" in load_data:
                assignment_data["eta_out"] = load_data["planned_departure"]

            assignment = await self.api_client.create_assignment(assignment_data)
            logger.info(f"Created assignment: {assignment}")

            self._update_status(f"✓ Dock {self.selected_dock.ramp_code} occupied with {modal_data['load_ref']}")
            await self.action_refresh()
        except Exception as exc:
            logger.exception("Failed to occupy dock")
            self._update_status(f"❌ Error: {exc}")

    def action_free_dock(self) -> None:
        """Free selected dock."""
        if not self.selected_dock:
            self._update_status("⚠️ Select a dock first")
            return

        if self.selected_dock.is_free:
            self._update_status("⚠️ Dock is already free")
            return

        self.run_worker(self._free_dock_async(), exclusive=True)

    async def _free_dock_async(self) -> None:
        """Async worker to free dock via API."""
        try:
            assignment_id = self.selected_dock.assignment_id
            if not assignment_id:
                self._update_status("❌ Error: No assignment to delete")
                return

            await self.api_client.delete_assignment(assignment_id)
            logger.info(f"Deleted assignment {assignment_id}")

            self._update_status(f"✓ Dock {self.selected_dock.ramp_code} freed")
            await self.action_refresh()
        except Exception as exc:
            logger.exception("Failed to free dock")
            self._update_status(f"❌ Error: {exc}")

    def action_block_dock(self) -> None:
        """Block selected dock with reason."""
        if not self.selected_dock:
            self._update_status("⚠️ Select a dock first")
            return

        def handle_result(result: Optional[Dict[str, Any]]) -> None:
            if result:
                self.run_worker(self._block_dock_async(result), exclusive=True)

        self.app.push_screen(BlockDockModal(self.selected_dock.ramp_code), callback=handle_result)

    async def _block_dock_async(self, modal_data: Dict[str, Any]) -> None:
        """Async worker to block dock via API."""
        try:
            # Get statuses to find CANCELLED status ID
            statuses = await self.api_client.get_statuses()
            cancelled_status = next((s for s in statuses if s.get("code") == "CANCELLED"), None)

            if not cancelled_status:
                self._update_status("❌ Error: CANCELLED status not found")
                return

            # Create a load with "BLOCKED" reference and the reason in notes
            load_data = {
                "reference": f"BLOCKED-{self.selected_dock.ramp_code}",
                "direction": "IB",  # Direction doesn't matter for blocked status
                "notes": f"BLOCKED: {modal_data['reason']}",
            }
            load = await self.api_client.create_load(load_data)
            logger.info(f"Created blocked load: {load}")

            # Create assignment with CANCELLED status
            assignment_data = {
                "ramp_id": self.selected_dock.ramp_id,
                "load_id": load["id"],
                "status_id": cancelled_status["id"],
            }
            assignment = await self.api_client.create_assignment(assignment_data)
            logger.info(f"Created blocked assignment: {assignment}")

            self._update_status(f"🔴 Dock {self.selected_dock.ramp_code} blocked: {modal_data['reason']}")
            await self.action_refresh()
        except Exception as exc:
            logger.exception("Failed to block dock")
            self._update_status(f"❌ Error: {exc}")

    def _add_dock(self) -> None:
        """Add new dock (admin only)."""
        def handle_result(result: Optional[Dict[str, Any]]) -> None:
            if result:
                self.run_worker(self._add_dock_async(result), exclusive=True)

        self.app.push_screen(AddDockModal(), callback=handle_result)

    async def _add_dock_async(self, modal_data: Dict[str, Any]) -> None:
        """Async worker to add dock via API."""
        try:
            # Create the ramp with direction and type
            direction_label = "Inbound" if modal_data["direction"] == "IB" else "Outbound"
            type_label = "Prime" if modal_data["type"] == "PRIME" else "Buffer"
            ramp_data = {
                "code": modal_data["code"],
                "direction": modal_data["direction"],
                "type": modal_data["type"],
                "description": modal_data.get("description") or f"{type_label} {direction_label} dock {modal_data['code']}",
            }
            ramp = await self.api_client.create_ramp(ramp_data)
            logger.info(f"Created ramp: {ramp}")

            self._update_status(f"✓ Dock {modal_data['code']} ({type_label} {direction_label}) added")
            await self.action_refresh()
        except Exception as exc:
            logger.exception("Failed to add dock")
            self._update_status(f"❌ Error: {exc}")

    def _add_user(self) -> None:
        """Add new user (admin only)."""
        def handle_result(result: Optional[Dict[str, Any]]) -> None:
            if result:
                self.run_worker(self._add_user_async(result), exclusive=True)

        self.app.push_screen(AddUserModal(), callback=handle_result)

    async def _add_user_async(self, modal_data: Dict[str, Any]) -> None:
        """Async worker to add user via API."""
        try:
            # Create the user
            user_data = {
                "email": modal_data["email"],
                "full_name": modal_data["full_name"],
                "password": modal_data["password"],
                "role": modal_data["role"],
                "is_active": True,
            }
            user = await self.api_client.create_user(user_data)
            logger.info(f"Created user: {user}")

            self._update_status(f"✓ User {modal_data['email']} added")
        except Exception as exc:
            logger.exception("Failed to add user")
            self._update_status(f"❌ Error: {exc}")

    def _update_tables(self) -> None:
        """Update both prime and buffer tables with filtered data."""
        filtered = self._apply_filters(self.ramp_infos)

        prime_infos = [info for info in filtered if self._is_prime_dock(info)]
        buffer_infos = [info for info in filtered if not self._is_prime_dock(info)]

        self._populate_table("#prime-table", prime_infos)
        self._populate_table("#buffer-table", buffer_infos)

    def _is_prime_dock(self, info: RampInfo) -> bool:
        """Determine if dock is prime based on admin-assigned type."""
        # Use the type field from the ramp (set by admin)
        return info.ramp_type == "PRIME"

    def _populate_table(self, table_id: str, infos: List[RampInfo]) -> None:
        """Populate a specific table with ramp info."""
        table = self.query_one(table_id, DataTable)
        table.clear()

        sorted_infos = self._sort_by_priority(infos)

        for info in sorted_infos:
            priority_icon = self._get_priority_icon(info)
            status_text = self._format_status(info)

            table.add_row(
                info.ramp_code,
                status_text,
                info.direction_label if info.direction else "-",
                info.load_ref or "-",
                self._format_eta(info),
                self._format_duration(info),
                priority_icon,
                (info.notes or "-")[:30],
                key=str(info.ramp_id),
            )

    def _sort_by_priority(self, infos: List[RampInfo]) -> List[RampInfo]:
        """Sort by priority."""
        def priority_key(info: RampInfo) -> tuple:
            if info.is_overdue:
                return (0, info.ramp_code)
            elif info.is_blocked:
                return (1, info.ramp_code)
            elif info.is_occupied:
                return (2, info.ramp_code)
            else:
                return (3, info.ramp_code)

        return sorted(infos, key=priority_key)

    def _apply_filters(self, infos: List[RampInfo]) -> List[RampInfo]:
        """Apply search and direction filters."""
        filtered = []
        query = self.search_query.lower().strip()

        for info in infos:
            if self.direction_filter == "IB" and info.direction != "IB":
                continue
            if self.direction_filter == "OB" and info.direction != "OB":
                continue
            if query and not info.matches_query(query):
                continue

            filtered.append(info)

        return filtered

    def _update_info_panel(self) -> None:
        """Update statistics in right panel."""
        total = len(self.ramp_infos)

        prime_infos = [info for info in self.ramp_infos if self._is_prime_dock(info)]
        buffer_infos = [info for info in self.ramp_infos if not self._is_prime_dock(info)]

        prime_free = sum(1 for info in prime_infos if info.is_free)
        prime_occupied = len(prime_infos) - prime_free

        buffer_free = sum(1 for info in buffer_infos if info.is_free)
        buffer_occupied = len(buffer_infos) - buffer_free

        ib_count = sum(1 for info in self.ramp_infos if info.direction == "IB")
        ob_count = sum(1 for info in self.ramp_infos if info.direction == "OB")

        urgent = sum(1 for info in self.ramp_infos if info.is_overdue)
        blocked = sum(1 for info in self.ramp_infos if info.is_blocked)

        panel = self.query_one(InfoPanel)
        panel.update_stats(
            total=total,
            prime_free=prime_free,
            prime_occupied=prime_occupied,
            buffer_free=buffer_free,
            buffer_occupied=buffer_occupied,
            ib_count=ib_count,
            ob_count=ob_count,
            urgent=urgent,
            blocked=blocked,
        )

    def _get_priority_icon(self, info: RampInfo) -> str:
        """Get priority icon."""
        if info.is_overdue:
            return "[red]🔴[/red]"
        elif info.is_blocked:
            return "[orange_red1]🟠[/orange_red1]"
        elif info.is_exception:
            return "[yellow]⚠️[/yellow]"
        elif info.is_occupied:
            return "[cyan]🟢[/cyan]"
        else:
            return "[dim]⚪[/dim]"

    def _format_status(self, info: RampInfo) -> str:
        """Format status with color."""
        label = info.status_label or "FREE"
        if info.is_blocked:
            return f"[red]{label}[/red]"
        elif info.is_overdue:
            return f"[orange_red1]{label}[/orange_red1]"
        elif info.is_occupied:
            return f"[yellow]{label}[/yellow]"
        else:
            return f"[green]{label}[/green]"

    def _format_eta(self, info: RampInfo) -> str:
        """Format ETA out."""
        dt = info.eta_out_dt
        if not dt:
            return "-"
        return dt.strftime("%H:%M")

    def _format_duration(self, info: RampInfo) -> str:
        """Return human friendly duration."""
        origin = info.created_at_dt or info.updated_at_dt
        if not origin:
            return "-"
        now = datetime.now(timezone.utc)
        delta = now - origin
        minutes = int(delta.total_seconds() // 60)
        if minutes < 60:
            return f"{minutes}m"
        hours = minutes // 60
        if hours < 24:
            return f"{hours}h"
        days = hours // 24
        return f"{days}d"

    async def _handle_ws_event(self, _: Dict[str, Any]) -> None:
        """React to WebSocket updates."""
        await self.action_refresh()

    async def on_input_changed(self, event: Input.Changed) -> None:
        """Handle search input changes."""
        if event.input.id == "search-input":
            self.search_query = event.value or ""
            self._update_tables()

    async def action_focus_search(self) -> None:
        """Focus search input."""
        search_input = self.query_one("#search-input", Input)
        await search_input.focus()

    async def action_filter_all(self) -> None:
        """Show all docks."""
        self.direction_filter = None
        self._update_tables()
        self._update_status("Filter: All")

    async def action_filter_ib(self) -> None:
        """Show only inbound."""
        self.direction_filter = "IB"
        self._update_tables()
        self._update_status("Filter: IB")

    async def action_filter_ob(self) -> None:
        """Show only outbound."""
        self.direction_filter = "OB"
        self._update_tables()
        self._update_status("Filter: OB")

    async def action_quit(self) -> None:
        """Exit to login screen."""
        await self.app.pop_screen()

    def _update_status(self, message: str) -> None:
        """Update status bar message."""
        label = self.query_one("#status-bar", Label)
        label.update(message)
