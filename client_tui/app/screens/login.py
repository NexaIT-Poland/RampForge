"""Login screen for RampForge TUI."""
from typing import Any

from textual.app import ComposeResult
from textual.containers import Center, Container, Vertical
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, Input, Label, Static

from app.services import APIClient, APIError


class LoginScreen(Screen):
    """
    Login screen for user authentication.

    Displays a centered login form with email and password input fields.
    On successful authentication, stores the JWT token in the APIClient
    and transitions to the main dashboard screen. Shows error messages
    for failed login attempts or server connectivity issues.

    Attributes:
        api_client: Shared APIClient instance for making authenticated requests
        email_input: Email address input widget
        password_input: Password input widget (masked)
        error_label: Label for displaying error messages
    """

    CSS = """
    LoginScreen {
        align: center middle;
    }

    #login-container {
        width: 60;
        height: auto;
        border: thick $background 80%;
        background: $surface;
        padding: 2 4;
    }

    #title {
        width: 100%;
        content-align: center middle;
        text-style: bold;
        color: $accent;
        margin-bottom: 1;
    }

    #subtitle {
        width: 100%;
        content-align: center middle;
        color: $text-muted;
        margin-bottom: 2;
    }

    Label {
        margin-top: 1;
        margin-bottom: 0;
    }

    Input {
        margin-bottom: 1;
    }

    #login-button {
        width: 100%;
        margin-top: 1;
    }

    #error {
        color: $error;
        text-align: center;
        margin-top: 1;
        height: auto;
    }

    #company-info {
        width: 100%;
        content-align: center middle;
        color: $text-muted;
        text-style: italic;
        margin-top: 2;
        padding-top: 1;
        border-top: solid $panel;
    }

    #company-contact {
        width: 100%;
        content-align: center middle;
        color: $accent;
        margin-top: 0;
    }

    Footer {
        background: $boost;
    }
    """

    BINDINGS = [
        ("escape", "quit", "Quit"),
    ]

    def __init__(self, api_client: APIClient) -> None:
        """Initialize login screen."""
        super().__init__()
        self.api_client = api_client

    def compose(self) -> ComposeResult:
        """Compose login screen."""
        yield Header()
        with Center():
            with Vertical(id="login-container"):
                yield Static("RampForge", id="title")
                yield Static("Distribution Center Dock Scheduling", id="subtitle")
                yield Label("Email:")
                yield Input(
                    placeholder="admin@rampforge.dev",
                    id="email",
                )
                yield Label("Password:")
                yield Input(
                    placeholder="Enter password",
                    password=True,
                    id="password",
                )
                yield Button("Login", variant="primary", id="login-button")
                yield Static("", id="error")
                yield Static("Made by NEXAIT sp. z o.o.", id="company-info")
                yield Static("📧 office@nexait.pl | 🌐 https://nexait.pl/", id="company-contact")
        yield Footer()

    def on_mount(self) -> None:
        """Focus email input on mount."""
        self.query_one("#email", Input).focus()

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        """Handle login button press."""
        if event.button.id == "login-button":
            await self.attempt_login()

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle input submission (Enter key)."""
        if event.input.id == "email":
            # Move to password field
            self.query_one("#password", Input).focus()
        elif event.input.id == "password":
            # Attempt login
            await self.attempt_login()

    async def attempt_login(self) -> None:
        """Attempt to login."""
        email_input = self.query_one("#email", Input)
        password_input = self.query_one("#password", Input)
        error_label = self.query_one("#error", Static)
        login_button = self.query_one("#login-button", Button)

        email = email_input.value.strip()
        password = password_input.value

        if not email or not password:
            error_label.update("Please enter email and password")
            return

        # Disable button during login
        login_button.disabled = True
        error_label.update("Logging in...")

        try:
            # Attempt login
            user_data = await self.api_client.login(email, password)

            # Success - dismiss screen with user data
            self.dismiss(user_data)

        except APIError as e:
            error_label.update(f"Login failed: {e.detail}")
            login_button.disabled = False
            password_input.value = ""
            password_input.focus()

        except Exception as e:
            error_label.update(f"Error: {str(e)}")
            login_button.disabled = False
