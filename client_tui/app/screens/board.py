"""Main board screen for assignments."""
from typing import Any, Dict, List, Optional

from textual.app import ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Button, DataTable, Footer, Header, Label, Static

from app.services import APIClient, WebSocketClient


class BoardScreen(Screen):
    """Main board screen showing all assignments."""

    CSS = """
    BoardScreen {
        layout: vertical;
    }

    #header-bar {
        width: 100%;
        height: 3;
        background: $boost;
        padding: 0 2;
        align-horizontal: space-between;
    }

    #user-info {
        width: auto;
        height: 100%;
        content-align: left middle;
        color: $text;
    }

    #filter-container {
        width: auto;
        height: 100%;
        align-horizontal: right;
    }

    .filter-button {
        margin: 0 1;
    }

    #status-bar {
        width: 100%;
        height: 1;
        background: $panel;
        padding: 0 2;
    }

    #assignments-table {
        width: 100%;
        height: 1fr;
    }

    Footer {
        background: $boost;
    }
    """

    BINDINGS = [
        ("r", "refresh", "Refresh"),
        ("n", "new_assignment", "New"),
        ("d", "delete_assignment", "Delete"),
        ("1", "filter_all", "All"),
        ("2", "filter_ib", "Inbound"),
        ("3", "filter_ob", "Outbound"),
        ("escape", "quit", "Quit"),
    ]

    def __init__(
        self,
        api_client: APIClient,
        ws_client: WebSocketClient,
        user_data: Dict[str, Any],
    ) -> None:
        """Initialize board screen."""
        super().__init__()
        self.api_client = api_client
        self.ws_client = ws_client
        self.user_data = user_data
        self.current_filter: Optional[str] = None
        self.assignments: List[Dict[str, Any]] = []

    def compose(self) -> ComposeResult:
        """Compose board screen."""
        yield Header()
        with Horizontal(id="header-bar"):
            yield Label(
                f"👤 {self.user_data.get('full_name')} ({self.user_data.get('role')})",
                id="user-info",
            )
            with Horizontal(id="filter-container"):
                yield Button("All", variant="primary", id="filter-all", classes="filter-button")
                yield Button("Inbound", id="filter-ib", classes="filter-button")
                yield Button("Outbound", id="filter-ob", classes="filter-button")
        yield Label("🔄 Connected to real-time updates", id="status-bar")
        yield DataTable(id="assignments-table", cursor_type="row")
        yield Footer()

    async def on_mount(self) -> None:
        """Initialize table and load data."""
        # Setup table
        table = self.query_one("#assignments-table", DataTable)
        table.add_columns(
            "ID", "Ramp", "Load", "Direction", "Status", "ETA In", "ETA Out", "Version"
        )

        # Connect WebSocket and register callbacks
        await self.ws_client.connect()
        self.ws_client.on_message("assignment_created", self._on_assignment_created)
        self.ws_client.on_message("assignment_updated", self._on_assignment_updated)
        self.ws_client.on_message("assignment_deleted", self._on_assignment_deleted)
        self.ws_client.on_message("conflict_detected", self._on_conflict_detected)

        # Load initial data
        await self.action_refresh()

    def _on_assignment_created(self, data: Dict[str, Any]) -> None:
        """Handle assignment created event."""
        assignment = data.get("data", {})
        self.assignments.append(assignment)
        self._refresh_table()
        self._update_status(f"✨ New assignment created by {data.get('user_email')}")

    def _on_assignment_updated(self, data: Dict[str, Any]) -> None:
        """Handle assignment updated event."""
        assignment = data.get("data", {})
        assignment_id = data.get("assignment_id")

        # Update in list
        for i, a in enumerate(self.assignments):
            if a.get("id") == assignment_id:
                self.assignments[i] = assignment
                break

        self._refresh_table()
        self._update_status(f"✏️ Assignment updated by {data.get('user_email')}")

    def _on_assignment_deleted(self, data: Dict[str, Any]) -> None:
        """Handle assignment deleted event."""
        assignment_id = data.get("assignment_id")

        # Remove from list
        self.assignments = [a for a in self.assignments if a.get("id") != assignment_id]

        self._refresh_table()
        self._update_status(f"🗑️ Assignment deleted by {data.get('user_email')}")

    def _on_conflict_detected(self, data: Dict[str, Any]) -> None:
        """Handle conflict detected event."""
        self._update_status(f"⚠️ Version conflict detected on assignment {data.get('assignment_id')}")

    def _update_status(self, message: str) -> None:
        """Update status bar."""
        status_bar = self.query_one("#status-bar", Label)
        status_bar.update(message)

    def _refresh_table(self) -> None:
        """Refresh table with current assignments."""
        table = self.query_one("#assignments-table", DataTable)
        table.clear()

        # Filter assignments
        filtered = self.assignments
        if self.current_filter:
            filtered = [
                a
                for a in self.assignments
                if a.get("load", {}).get("direction") == self.current_filter
            ]

        # Add rows
        for assignment in filtered:
            ramp = assignment.get("ramp", {})
            load = assignment.get("load", {})
            status = assignment.get("status", {})

            table.add_row(
                str(assignment.get("id", "")),
                ramp.get("code", ""),
                load.get("reference", ""),
                load.get("direction", ""),
                status.get("label", ""),
                self._format_datetime(assignment.get("eta_in")),
                self._format_datetime(assignment.get("eta_out")),
                str(assignment.get("version", "")),
                key=str(assignment.get("id")),
            )

    def _format_datetime(self, dt: Optional[str]) -> str:
        """Format datetime string."""
        if not dt:
            return "-"
        # Simple format - just show date and time
        try:
            return dt.split("T")[0] + " " + dt.split("T")[1][:5]
        except:
            return dt[:16]

    async def action_refresh(self) -> None:
        """Refresh assignments from API."""
        try:
            self.assignments = await self.api_client.get_assignments(self.current_filter)
            self._refresh_table()
            self._update_status(f"✓ Loaded {len(self.assignments)} assignments")
        except Exception as e:
            self._update_status(f"❌ Error loading assignments: {e}")

    async def action_new_assignment(self) -> None:
        """Create new assignment."""
        self._update_status("ℹ️ New assignment dialog not yet implemented")

    async def action_delete_assignment(self) -> None:
        """Delete selected assignment."""
        table = self.query_one("#assignments-table", DataTable)
        if table.cursor_row is None:
            self._update_status("⚠️ No assignment selected")
            return

        row_key = table.get_row_at(table.cursor_row)
        assignment_id = int(row_key[0])

        try:
            await self.api_client.delete_assignment(assignment_id)
            self._update_status(f"✓ Deleted assignment {assignment_id}")
            # WebSocket will handle the update
        except Exception as e:
            self._update_status(f"❌ Error deleting assignment: {e}")

    async def action_filter_all(self) -> None:
        """Show all assignments."""
        self.current_filter = None
        await self.ws_client.unsubscribe()
        await self.action_refresh()
        self._update_buttons("filter-all")

    async def action_filter_ib(self) -> None:
        """Show inbound assignments only."""
        self.current_filter = "IB"
        await self.ws_client.subscribe("IB")
        await self.action_refresh()
        self._update_buttons("filter-ib")

    async def action_filter_ob(self) -> None:
        """Show outbound assignments only."""
        self.current_filter = "OB"
        await self.ws_client.subscribe("OB")
        await self.action_refresh()
        self._update_buttons("filter-ob")

    def _update_buttons(self, active_id: str) -> None:
        """Update filter button variants."""
        for button_id in ["filter-all", "filter-ib", "filter-ob"]:
            button = self.query_one(f"#{button_id}", Button)
            button.variant = "primary" if button_id == active_id else "default"
