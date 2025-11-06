"""RampForge TUI Application."""
import argparse
from typing import Any, Dict, Optional

from textual.app import App

from app.screens import DockDashboardScreen, EnhancedDockDashboard, LoginScreen
from app.services import APIClient, WebSocketClient


class RampForgeApp(App):
    """RampForge Terminal User Interface Application."""

    CSS = """
    Screen {
        background: $surface;
    }
    """

    def __init__(self, api_url: str, ws_url: str, use_legacy_ui: bool = False) -> None:
        """Initialize RampForge app."""
        super().__init__()
        self.api_client = APIClient(api_url)
        self.ws_client = WebSocketClient(ws_url)
        self.user_data: Optional[Dict[str, Any]] = None
        self.use_legacy_ui = use_legacy_ui

    async def on_mount(self) -> None:
        """Show login screen on startup."""
        await self.push_screen(LoginScreen(self.api_client), callback=self.on_login_success)

    def on_login_success(self, user_data: Optional[Dict[str, Any]]) -> None:
        """Handle successful login."""
        if user_data and self.api_client.token:
            self.ws_client.set_token(self.api_client.token)

            # Choose dashboard based on user preference
            if self.use_legacy_ui:
                dashboard = DockDashboardScreen(self.api_client, self.ws_client, user_data)
            else:
                dashboard = EnhancedDockDashboard(self.api_client, self.ws_client, user_data)

            self.push_screen(dashboard)


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="RampForge Terminal User Interface")
    parser.add_argument(
        "--api-url",
        default="http://localhost:8000",
        help="API server URL (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--ws-url",
        default="ws://localhost:8000",
        help="WebSocket server URL (default: ws://localhost:8000)",
    )
    parser.add_argument(
        "--legacy-ui",
        action="store_true",
        help="Use legacy dashboard UI instead of enhanced UI",
    )

    args = parser.parse_args()

    app = RampForgeApp(args.api_url, args.ws_url, use_legacy_ui=args.legacy_ui)
    app.run()


if __name__ == "__main__":
    main()
