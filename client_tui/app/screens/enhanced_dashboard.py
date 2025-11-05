"""Enhanced dock dashboard with tabbed IB/OB views and grouping."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from collections import defaultdict

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical, Container
from textual.screen import Screen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    Select,
    TabbedContent,
    TabPane,
    Static,
)

from app.services import APIClient, WebSocketClient
from app.services.ramp_status import RampInfo, get_ramp_statuses
from app.widgets import RampDetailPanel
from app.widgets.filter_sidebar import SidebarSummary

logger = logging.getLogger(__name__)


class SummaryCard(Static):
    """Card widget for displaying key metrics."""

    DEFAULT_CSS = """
    SummaryCard {
        width: 1fr;
        height: 7;
        background: $surface-darken-2;
        border: solid $panel;
        border-radius: 1;
        padding: 1;
        content-align: center middle;
    }

    SummaryCard .card-value {
        text-style: bold;
        color: $primary;
    }

    SummaryCard .card-label {
        color: $text-muted;
    }
    """

    def __init__(self, label: str, value: str, color: str = "cyan", *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.label = label
        self.value = value
        self.color = color

    def compose(self) -> ComposeResult:
        """Compose card layout."""
        yield Static(f"[{self.color}]{self.value}[/{self.color}]", classes="card-value")
        yield Static(self.label, classes="card-label")

    def update_card(self, value: str, color: str = "cyan") -> None:
        """Update card value and color."""
        self.value = value
        self.color = color
        card_value = self.query_one(".card-value", Static)
        card_value.update(f"[{self.color}]{value}[/{self.color}]")


class GroupedDataView(Static):
    """Widget that displays data grouped by status with collapsible sections."""

    DEFAULT_CSS = """
    GroupedDataView {
        width: 100%;
        height: 1fr;
        background: $surface-darken-1;
        overflow-y: auto;
        padding: 1;
    }

    GroupedDataView .group-header {
        width: 100%;
        height: 3;
        background: $boost;
        color: $text;
        text-style: bold;
        content-align: left middle;
        padding: 0 2;
        margin-bottom: 1;
    }

    GroupedDataView .group-table {
        width: 100%;
        margin-bottom: 2;
    }

    GroupedDataView .empty-message {
        width: 100%;
        height: 5;
        content-align: center middle;
        color: $text-muted;
    }
    """

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.groups: Dict[str, List[RampInfo]] = {}
        self.sort_column: Optional[str] = None
        self.sort_reverse: bool = False

    def compose(self) -> ComposeResult:
        """Compose grouped view."""
        yield Static("No data to display", classes="empty-message")

    def update_groups(self, ramp_infos: List[RampInfo]) -> None:
        """Update grouped data display."""
        # Clear existing content
        self.remove_children()

        if not ramp_infos:
            with Vertical():
                yield Static("📭 No assignments found", classes="empty-message")
            return

        # Group by status
        self.groups = defaultdict(list)
        for info in ramp_infos:
            status_key = info.status_label or "FREE"
            self.groups[status_key].append(info)

        # Sort groups by priority: DELAYED > IN_PROGRESS > PLANNED > others
        priority_order = {
            "Delayed": 0,
            "In Progress": 1,
            "Arrived": 2,
            "Planned": 3,
            "Completed": 4,
            "Cancelled": 5,
            "FREE": 6,
        }

        sorted_groups = sorted(
            self.groups.items(), key=lambda x: priority_order.get(x[0], 99)
        )

        # Create groups
        with Vertical():
            for status_label, infos in sorted_groups:
                count = len(infos)

                # Determine group color
                if status_label == "Delayed":
                    color = "red"
                elif status_label == "In Progress":
                    color = "yellow"
                elif status_label == "Planned":
                    color = "cyan"
                elif status_label == "Arrived":
                    color = "blue"
                else:
                    color = "white"

                # Group header
                yield Static(
                    f"[{color}]● {status_label}[/{color}] ({count})",
                    classes="group-header"
                )

                # Group table
                table = DataTable(
                    cursor_type="row",
                    classes="group-table"
                )
                table.add_columns(
                    "Ramp",
                    "Load Ref",
                    "Direction",
                    "ETA Out",
                    "Duration",
                    "Priority",
                    "Notes"
                )

                # Sort infos within group
                sorted_infos = self._sort_infos(infos)

                for info in sorted_infos:
                    priority_icon = self._get_priority_icon(info)
                    table.add_row(
                        info.ramp_code,
                        info.load_ref or "-",
                        info.direction_label,
                        self._format_eta(info),
                        self._format_duration(info),
                        priority_icon,
                        (info.notes or "-")[:30],
                        key=str(info.ramp_id)
                    )

                yield table

    def _sort_infos(self, infos: List[RampInfo]) -> List[RampInfo]:
        """Sort infos by priority: overdue > blocked > occupied > free."""
        def priority_key(info: RampInfo) -> tuple:
            # Higher priority = lower number
            if info.is_overdue:
                return (0, info.ramp_code)
            elif info.is_blocked:
                return (1, info.ramp_code)
            elif info.is_occupied:
                return (2, info.ramp_code)
            else:
                return (3, info.ramp_code)

        return sorted(infos, key=priority_key)

    def _get_priority_icon(self, info: RampInfo) -> str:
        """Get priority icon based on ramp state."""
        if info.is_overdue:
            return "[red]🔴 URGENT[/red]"
        elif info.is_blocked:
            return "[orange_red1]🟠 BLOCKED[/orange_red1]"
        elif info.is_exception:
            return "[yellow]⚠️  WARN[/yellow]"
        elif info.is_occupied:
            return "[cyan]🟢 OK[/cyan]"
        else:
            return "[dim]⚪ FREE[/dim]"

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


class EnhancedDockDashboard(Screen):
    """Enhanced dashboard with tabbed IB/OB views and status grouping."""

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
        column-gap: 2;
    }

    #user-info {
        width: 1fr;
        height: 100%;
        content-align: left middle;
        color: $text;
    }

    #header-title {
        width: auto;
        color: $text-muted;
        content-align: right middle;
    }

    #summary-cards {
        width: 100%;
        height: 9;
        layout: horizontal;
        padding: 1 2;
        column-gap: 1;
        background: $surface;
    }

    #filter-bar {
        width: 100%;
        height: 3;
        padding: 0 2;
        layout: horizontal;
        align: center middle;
        column-gap: 1;
        background: $surface-darken-1;
        border-bottom: solid $boost;
    }

    #status-bar {
        width: 100%;
        height: 1;
        background: $panel;
        padding: 0 2;
        content-align: left middle;
        color: $text;
    }

    #main-container {
        width: 100%;
        height: 1fr;
        layout: horizontal;
    }

    #content-area {
        width: 1fr;
        height: 100%;
    }

    TabbedContent {
        width: 100%;
        height: 100%;
    }

    TabPane {
        padding: 0;
    }

    #search-input {
        width: 30;
    }

    Footer {
        background: $boost;
    }
    """

    BINDINGS = [
        ("r", "refresh", "Refresh"),
        ("escape", "quit", "Quit"),
        ("ctrl+f", "focus_search", "Search"),
        ("ctrl+1", "switch_tab_all", "All"),
        ("ctrl+2", "switch_tab_ib", "IB Only"),
        ("ctrl+3", "switch_tab_ob", "OB Only"),
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
        self.ramps: List[Dict[str, Any]] = []
        self.assignments: List[Dict[str, Any]] = []
        self.ramp_infos: List[RampInfo] = []
        self.search_query: str = ""
        self.status_filter: Optional[str] = None

    def compose(self) -> ComposeResult:
        """Compose enhanced dashboard layout."""
        yield Header()

        # Header bar
        with Horizontal(id="header-bar"):
            yield Label(
                f"👤 {self.user_data.get('full_name', 'User')} ({self.user_data.get('role', '')})",
                id="user-info",
            )
            yield Label("🚀 Enhanced Operations Dashboard", id="header-title")

        # Summary cards
        with Horizontal(id="summary-cards"):
            yield SummaryCard("Total Docks", "0", "cyan")
            yield SummaryCard("Inbound (IB)", "0", "blue")
            yield SummaryCard("Outbound (OB)", "0", "yellow")
            yield SummaryCard("🔴 Urgent", "0", "red")
            yield SummaryCard("⚠️  Blocked", "0", "orange_red1")
            yield SummaryCard("🟢 Free", "0", "green")

        # Filter bar
        with Horizontal(id="filter-bar"):
            yield Select([], prompt="Filter by Status", id="status-select", allow_blank=True)
            yield Input(placeholder="🔍 Search ramp, load, notes...", id="search-input")
            yield Button("Clear Filters", id="btn-clear-filters", variant="default")

        yield Label("🔄 Initializing...", id="status-bar")

        # Main content with tabs
        with Horizontal(id="main-container"):
            with Container(id="content-area"):
                with TabbedContent(initial="tab-all"):
                    with TabPane("📊 All Docks", id="tab-all"):
                        yield GroupedDataView(id="view-all")
                    with TabPane("📥 Inbound (IB)", id="tab-ib"):
                        yield GroupedDataView(id="view-ib")
                    with TabPane("📤 Outbound (OB)", id="tab-ob"):
                        yield GroupedDataView(id="view-ob")

            yield RampDetailPanel()

        yield Footer()

    async def on_mount(self) -> None:
        """Initialize dashboard and connect WebSocket."""
        logger.info("EnhancedDockDashboard mount started")

        # Connect WebSocket
        try:
            await self.ws_client.connect()
            self.ws_client.on_message("assignment_created", self._handle_assignment_event)
            self.ws_client.on_message("assignment_updated", self._handle_assignment_event)
            self.ws_client.on_message("assignment_deleted", self._handle_assignment_event)
            self._update_status("🔗 Connected to live updates")
        except Exception as exc:
            logger.exception("WebSocket connection failed")
            self._update_status(f"⚠️ Offline mode – WebSocket error: {exc}")

        await self.action_refresh()
        logger.info("EnhancedDockDashboard mount completed")

    async def on_unmount(self) -> None:
        """Clean up WebSocket subscriptions."""
        try:
            await self.ws_client.disconnect()
        except Exception:
            logger.exception("Failed to disconnect WebSocket cleanly")

    async def action_refresh(self) -> None:
        """Reload all data from API."""
        logger.info("Refreshing enhanced dashboard data")
        try:
            self.ramps = await self.api_client.get_ramps()
            self.assignments = await self.api_client.get_assignments()
        except Exception as exc:
            logger.exception("Failed to load data")
            self._update_status(f"❌ Error loading data: {exc}")
            return

        self.ramp_infos = get_ramp_statuses(self.ramps, self.assignments)
        self._update_all_views()
        self._update_summary_cards()
        self._hydrate_status_options()
        self._update_status(f"✓ Loaded {len(self.ramp_infos)} docks")

    def _update_all_views(self) -> None:
        """Update all tab views with filtered data."""
        # Apply filters
        filtered = self._apply_filters(self.ramp_infos)

        # All view
        view_all = self.query_one("#view-all", GroupedDataView)
        view_all.update_groups(filtered)

        # IB view
        ib_infos = [info for info in filtered if info.direction == "IB"]
        view_ib = self.query_one("#view-ib", GroupedDataView)
        view_ib.update_groups(ib_infos)

        # OB view
        ob_infos = [info for info in filtered if info.direction == "OB"]
        view_ob = self.query_one("#view-ob", GroupedDataView)
        view_ob.update_groups(ob_infos)

    def _apply_filters(self, infos: List[RampInfo]) -> List[RampInfo]:
        """Apply search and status filters."""
        filtered = []
        query = self.search_query.lower().strip()

        for info in infos:
            if self.status_filter and info.status_code != self.status_filter:
                continue
            if query and not info.matches_query(query):
                continue
            filtered.append(info)

        return filtered

    def _update_summary_cards(self) -> None:
        """Update summary cards with current stats."""
        total = len(self.ramp_infos)
        ib_count = sum(1 for info in self.ramp_infos if info.direction == "IB")
        ob_count = sum(1 for info in self.ramp_infos if info.direction == "OB")
        urgent = sum(1 for info in self.ramp_infos if info.is_overdue)
        blocked = sum(1 for info in self.ramp_infos if info.is_blocked)
        free = sum(1 for info in self.ramp_infos if info.is_free)

        cards = self.query(SummaryCard)
        if len(cards) >= 6:
            cards[0].update_card(str(total), "cyan")
            cards[1].update_card(str(ib_count), "blue")
            cards[2].update_card(str(ob_count), "yellow")
            cards[3].update_card(str(urgent), "red" if urgent > 0 else "dim")
            cards[4].update_card(str(blocked), "orange_red1" if blocked > 0 else "dim")
            cards[5].update_card(str(free), "green")

    def _hydrate_status_options(self) -> None:
        """Populate status dropdown."""
        status_map: Dict[str, str] = {}
        for info in self.ramp_infos:
            code = info.status_code or "FREE"
            status_map[code] = info.status_label

        select = self.query_one("#status-select", Select)
        select.set_options([(label, code) for code, label in sorted(status_map.items())])
        if self.status_filter and self.status_filter in status_map:
            select.value = self.status_filter

    async def _handle_assignment_event(self, _: Dict[str, Any]) -> None:
        """React to WebSocket updates."""
        await self.action_refresh()

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle button presses."""
        if event.button.id == "btn-clear-filters":
            self.search_query = ""
            self.status_filter = None
            self.query_one("#search-input", Input).value = ""
            self.query_one("#status-select", Select).clear()
            self._update_all_views()

    async def on_select_changed(self, event: Select.Changed) -> None:
        """Handle status filter changes."""
        if event.select.id == "status-select":
            self.status_filter = event.value or None
            self._update_all_views()

    async def on_input_changed(self, event: Input.Changed) -> None:
        """Handle search input changes."""
        if event.input.id == "search-input":
            self.search_query = event.value or ""
            self._update_all_views()

    async def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        """Update detail panel when row selected."""
        if not event.row_key:
            return

        info = self._get_ramp_info_by_key(event.row_key.value)
        detail = self.query_one(RampDetailPanel)
        detail.update_detail(info)

    def _get_ramp_info_by_key(self, key: str) -> Optional[RampInfo]:
        """Find ramp info by key."""
        for info in self.ramp_infos:
            if str(info.ramp_id) == str(key):
                return info
        return None

    async def action_focus_search(self) -> None:
        """Focus search input."""
        search_input = self.query_one("#search-input", Input)
        await search_input.focus()

    async def action_switch_tab_all(self) -> None:
        """Switch to All tab."""
        tabbed = self.query_one(TabbedContent)
        tabbed.active = "tab-all"

    async def action_switch_tab_ib(self) -> None:
        """Switch to IB tab."""
        tabbed = self.query_one(TabbedContent)
        tabbed.active = "tab-ib"

    async def action_switch_tab_ob(self) -> None:
        """Switch to OB tab."""
        tabbed = self.query_one(TabbedContent)
        tabbed.active = "tab-ob"

    async def action_quit(self) -> None:
        """Exit to login screen."""
        await self.app.pop_screen()

    def _update_status(self, message: str) -> None:
        """Update status bar message."""
        label = self.query_one("#status-bar", Label)
        label.update(message)
