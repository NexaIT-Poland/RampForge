#!/usr/bin/env python3
"""WebSocket test client for DCDock real-time updates."""
import asyncio
import json
from datetime import datetime

import httpx
import websockets


async def get_auth_token(email: str, password: str, base_url: str = "http://localhost:8000") -> str:
    """
    Authenticate and get JWT token.

    Args:
        email: User email
        password: User password
        base_url: API base URL

    Returns:
        JWT token
    """
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{base_url}/api/auth/login",
            json={"email": email, "password": password},
        )
        response.raise_for_status()
        data = response.json()
        return data["access_token"]


async def websocket_listener(
    token: str, ws_url: str = "ws://localhost:8000/api/ws", direction_filter: str = None
) -> None:
    """
    Connect to WebSocket and listen for updates.

    Args:
        token: JWT token
        ws_url: WebSocket URL
        direction_filter: Optional direction filter (IB or OB)
    """
    uri = f"{ws_url}?token={token}"

    try:
        async with websockets.connect(uri) as websocket:
            print(f"Connected to {ws_url}")
            print("=" * 60)

            # Wait for connection acknowledgment
            message = await websocket.recv()
            data = json.loads(message)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Connection ACK:")
            print(f"  Client ID: {data.get('client_id')}")
            print(f"  Message: {data.get('message')}")
            print()

            # Send subscription with optional filter
            if direction_filter:
                subscribe_msg = {
                    "type": "subscribe",
                    "filters": {"direction": direction_filter},
                }
                await websocket.send(json.dumps(subscribe_msg))
                print(f"Subscribed with filter: direction={direction_filter}")
                print()

                # Wait for subscription acknowledgment
                ack = await websocket.recv()
                ack_data = json.loads(ack)
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Subscription ACK:")
                print(f"  {json.dumps(ack_data, indent=2)}")
                print()

            print("Listening for real-time updates...")
            print("=" * 60)
            print()

            # Listen for messages
            async for message in websocket:
                data = json.loads(message)
                msg_type = data.get("type")
                timestamp = data.get("timestamp")

                if msg_type == "assignment_created":
                    print(f"[{timestamp}] 🆕 NEW ASSIGNMENT CREATED")
                    print(f"  Assignment ID: {data.get('assignment_id')}")
                    print(f"  User: {data.get('user_email')}")
                    assignment_data = data.get("data", {})
                    print(f"  Ramp: {assignment_data.get('ramp', {}).get('code')}")
                    print(f"  Load: {assignment_data.get('load', {}).get('reference')}")
                    print(f"  Status: {assignment_data.get('status', {}).get('label')}")
                    print()

                elif msg_type == "assignment_updated":
                    print(f"[{timestamp}] ✏️  ASSIGNMENT UPDATED")
                    print(f"  Assignment ID: {data.get('assignment_id')}")
                    print(f"  User: {data.get('user_email')}")
                    assignment_data = data.get("data", {})
                    print(f"  Ramp: {assignment_data.get('ramp', {}).get('code')}")
                    print(f"  Load: {assignment_data.get('load', {}).get('reference')}")
                    print(f"  Status: {assignment_data.get('status', {}).get('label')}")
                    print(f"  Version: {assignment_data.get('version')}")
                    print()

                elif msg_type == "assignment_deleted":
                    print(f"[{timestamp}] 🗑️  ASSIGNMENT DELETED")
                    print(f"  Assignment ID: {data.get('assignment_id')}")
                    print(f"  User: {data.get('user_email')}")
                    print()

                elif msg_type == "conflict_detected":
                    print(f"[{timestamp}] ⚠️  CONFLICT DETECTED")
                    print(f"  Assignment ID: {data.get('assignment_id')}")
                    print(f"  Current Version: {data.get('current_version')}")
                    print(f"  Attempted Version: {data.get('attempted_version')}")
                    print(f"  Message: {data.get('message')}")
                    print()

                elif msg_type == "error":
                    print(f"[{timestamp}] ❌ ERROR")
                    print(f"  Message: {data.get('message')}")
                    if data.get("details"):
                        print(f"  Details: {data.get('details')}")
                    print()

                else:
                    print(f"[{timestamp}] Unknown message type: {msg_type}")
                    print(f"  {json.dumps(data, indent=2)}")
                    print()

    except websockets.exceptions.WebSocketException as e:
        print(f"WebSocket error: {e}")
    except KeyboardInterrupt:
        print("\nDisconnecting...")
    except Exception as e:
        print(f"Error: {e}")


async def main() -> None:
    """Main entry point."""
    import sys

    # Parse command-line arguments
    email = sys.argv[1] if len(sys.argv) > 1 else "admin@dcdock.com"
    password = sys.argv[2] if len(sys.argv) > 2 else "admin123"
    direction = sys.argv[3] if len(sys.argv) > 3 else None

    print("DCDock WebSocket Test Client")
    print("=" * 60)
    print(f"Email: {email}")
    print(f"Direction filter: {direction or 'None (all updates)'}")
    print()

    # Get authentication token
    print("Authenticating...")
    try:
        token = await get_auth_token(email, password)
        print("✓ Authentication successful")
        print()
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return

    # Connect to WebSocket
    await websocket_listener(token, direction_filter=direction)


if __name__ == "__main__":
    print()
    print("Usage: python test_websocket_client.py [email] [password] [direction]")
    print("  email: User email (default: admin@dcdock.com)")
    print("  password: User password (default: admin123)")
    print("  direction: Optional filter IB or OB (default: None)")
    print()
    print("Examples:")
    print("  python test_websocket_client.py")
    print("  python test_websocket_client.py admin@dcdock.com admin123")
    print("  python test_websocket_client.py operator1@dcdock.com operator123 IB")
    print()

    asyncio.run(main())
