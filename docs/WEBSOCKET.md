# WebSocket Real-Time Updates

DCDock provides real-time updates via WebSocket for all assignment changes. This enables multiple operators to see live board updates without manual refreshing.

## Features

- **Real-time assignment updates**: Instant notifications for CREATE, UPDATE, DELETE operations
- **Conflict notifications**: Alerts when optimistic locking detects version conflicts
- **Direction filtering**: Subscribe to only INBOUND or OUTBOUND loads
- **Multi-client support**: Up to 20+ concurrent WebSocket connections
- **Authenticated connections**: JWT token-based authentication

## Connection

### WebSocket Endpoint

```
ws://localhost:8000/api/ws?token=YOUR_JWT_TOKEN
```

### Authentication

1. First, obtain a JWT token via REST API:

```bash
curl -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@dcdock.com","password":"admin123"}'
```

2. Use the returned `access_token` in the WebSocket URL query parameter:

```javascript
const token = "eyJhbGciOiJIUzI1NiIs...";
const ws = new WebSocket(`ws://localhost:8000/api/ws?token=${token}`);
```

## Message Types

### Client → Server

#### Subscribe

Subscribe with optional filters:

```json
{
  "type": "subscribe",
  "filters": {
    "direction": "IB"  // Optional: "IB" or "OB"
  }
}
```

#### Unsubscribe

Clear all filters:

```json
{
  "type": "unsubscribe"
}
```

#### Ping

Keep-alive ping:

```json
{
  "type": "ping"
}
```

Response:

```json
{
  "type": "pong",
  "timestamp": "2025-01-01T00:00:00"
}
```

### Server → Client

#### Connection Acknowledgment

Sent immediately after connection:

```json
{
  "type": "connection_ack",
  "timestamp": "2025-01-01T00:00:00",
  "client_id": "user_1_12345",
  "message": "Connected to DCDock real-time updates"
}
```

#### Assignment Created

```json
{
  "type": "assignment_created",
  "timestamp": "2025-01-01T12:30:00",
  "assignment_id": 7,
  "action": "CREATE",
  "user_id": 3,
  "user_email": "operator1@dcdock.com",
  "data": {
    "id": 7,
    "ramp_id": 1,
    "load_id": 9,
    "status_id": 1,
    "eta_in": "2025-01-01T14:00:00",
    "eta_out": "2025-01-01T16:00:00",
    "version": 1,
    "ramp": {
      "id": 1,
      "code": "R1",
      "description": "Ramp 1 - Loading Bay A"
    },
    "load": {
      "id": 9,
      "reference": "IB-2025-005",
      "direction": "IB",
      "planned_arrival": "2025-01-01T14:00:00",
      "notes": "Electronics from Supplier E"
    },
    "status": {
      "id": 1,
      "code": "PLANNED",
      "label": "Planned",
      "color": "blue"
    },
    "creator": {
      "id": 3,
      "email": "operator1@dcdock.com",
      "full_name": "John Operator"
    },
    "updater": {
      "id": 3,
      "email": "operator1@dcdock.com",
      "full_name": "John Operator"
    }
  }
}
```

#### Assignment Updated

```json
{
  "type": "assignment_updated",
  "timestamp": "2025-01-01T12:35:00",
  "assignment_id": 7,
  "action": "UPDATE",
  "user_id": 4,
  "user_email": "operator2@dcdock.com",
  "data": {
    // Full assignment data (same structure as created)
    "version": 2  // Note: version incremented
  }
}
```

#### Assignment Deleted

```json
{
  "type": "assignment_deleted",
  "timestamp": "2025-01-01T12:40:00",
  "assignment_id": 7,
  "action": "DELETE",
  "user_id": 1,
  "user_email": "admin@dcdock.com",
  "data": {
    // Full assignment data before deletion
  }
}
```

#### Conflict Detected

Sent when optimistic locking detects a version conflict:

```json
{
  "type": "conflict_detected",
  "timestamp": "2025-01-01T12:45:00",
  "assignment_id": 7,
  "current_version": 5,
  "attempted_version": 4,
  "current_data": {
    // Full current assignment data
  },
  "message": "Another user has modified this assignment"
}
```

#### Error

```json
{
  "type": "error",
  "timestamp": "2025-01-01T12:50:00",
  "message": "Invalid message format",
  "details": "Expected JSON object with 'type' field"
}
```

## Usage Examples

### Python (websockets library)

```python
import asyncio
import json
import websockets
import httpx

async def listen_for_updates():
    # Get authentication token
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "http://localhost:8000/api/auth/login",
            json={"email": "admin@dcdock.com", "password": "admin123"}
        )
        token = response.json()["access_token"]

    # Connect to WebSocket
    uri = f"ws://localhost:8000/api/ws?token={token}"
    async with websockets.connect(uri) as ws:
        # Receive connection ACK
        ack = await ws.recv()
        print(f"Connected: {ack}")

        # Subscribe to INBOUND only
        await ws.send(json.dumps({
            "type": "subscribe",
            "filters": {"direction": "IB"}
        }))

        # Listen for updates
        async for message in ws:
            data = json.loads(message)
            print(f"Update: {data['type']} - Assignment {data.get('assignment_id')}")

asyncio.run(listen_for_updates())
```

### JavaScript (Browser)

```javascript
// Get token first
const loginResponse = await fetch('http://localhost:8000/api/auth/login', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    email: 'admin@dcdock.com',
    password: 'admin123'
  })
});
const { access_token } = await loginResponse.json();

// Connect to WebSocket
const ws = new WebSocket(`ws://localhost:8000/api/ws?token=${access_token}`);

ws.onopen = () => {
  console.log('Connected to DCDock WebSocket');

  // Subscribe to OUTBOUND only
  ws.send(JSON.stringify({
    type: 'subscribe',
    filters: { direction: 'OB' }
  }));
};

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);

  switch(data.type) {
    case 'connection_ack':
      console.log('Connection acknowledged:', data.client_id);
      break;

    case 'assignment_created':
      console.log('New assignment:', data.assignment_id);
      // Update UI with new assignment
      updateBoardUI(data.data);
      break;

    case 'assignment_updated':
      console.log('Assignment updated:', data.assignment_id);
      // Update UI with changed assignment
      updateBoardUI(data.data);
      break;

    case 'assignment_deleted':
      console.log('Assignment deleted:', data.assignment_id);
      // Remove from UI
      removeFromBoardUI(data.assignment_id);
      break;

    case 'conflict_detected':
      console.warn('Conflict detected:', data.message);
      // Show conflict dialog with current_data
      showConflictDialog(data);
      break;

    case 'error':
      console.error('Error:', data.message);
      break;
  }
};

ws.onerror = (error) => {
  console.error('WebSocket error:', error);
};

ws.onclose = () => {
  console.log('WebSocket connection closed');
  // Implement reconnection logic
};
```

## Testing

### Using the Test Client

A Python test client is provided:

```bash
# Listen to all updates
python backend/test_websocket_client.py

# Listen with specific user
python backend/test_websocket_client.py admin@dcdock.com admin123

# Listen to INBOUND only
python backend/test_websocket_client.py admin@dcdock.com admin123 IB

# Listen to OUTBOUND only
python backend/test_websocket_client.py operator1@dcdock.com operator123 OB
```

### Manual Testing

1. Start the backend server:

```bash
cd backend
python run.py
```

2. In another terminal, start the WebSocket test client:

```bash
python backend/test_websocket_client.py
```

3. In a third terminal, make API changes:

```bash
# Get token
TOKEN=$(curl -s -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@dcdock.com","password":"admin123"}' | jq -r '.access_token')

# Create assignment
curl -X POST http://localhost:8000/api/assignments/ \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "ramp_id": 1,
    "load_id": 1,
    "status_id": 1,
    "eta_in": "2025-01-01T14:00:00",
    "eta_out": "2025-01-01T16:00:00"
  }'
```

You should see real-time updates in the WebSocket client terminal.

## WebSocket Statistics

Monitor active connections:

```bash
curl http://localhost:8000/api/ws/stats
```

Response:

```json
{
  "active_connections": 3,
  "clients": [
    {
      "client_id": "user_1_12345",
      "filters": {"direction": "IB"}
    },
    {
      "client_id": "user_3_67890",
      "filters": {}
    },
    {
      "client_id": "user_4_11111",
      "filters": {"direction": "OB"}
    }
  ]
}
```

## Implementation Details

### Broadcast Filtering

The WebSocket manager automatically filters broadcasts based on client subscriptions:

- **No filter**: Receives all assignment updates
- **Direction filter** (`{"direction": "IB"}`): Only receives updates for INBOUND loads
- **Direction filter** (`{"direction": "OB"}`): Only receives updates for OUTBOUND loads

### Connection Management

- Connections are identified by unique client IDs
- Disconnected clients are automatically cleaned up
- Failed sends trigger client disconnection
- No message queuing (if client is offline, messages are missed)

### Concurrency

- Thread-safe connection management with asyncio locks
- Broadcasts are non-blocking
- Failed broadcasts to individual clients don't affect others

## Error Handling

### Connection Errors

- **401 Policy Violation**: Invalid or missing JWT token
- **1011 Internal Error**: Server-side error during message handling

### Message Errors

Invalid messages receive error responses but don't disconnect the client:

```json
{
  "type": "error",
  "message": "Invalid message format",
  "details": "..."
}
```

## Best Practices

1. **Implement reconnection logic**: WebSocket connections can drop
2. **Handle conflict notifications**: Show UI dialogs for version conflicts
3. **Use heartbeat/ping**: Send periodic pings to keep connection alive
4. **Validate message types**: Always check the `type` field before processing
5. **Store current version**: Keep track of assignment versions for updates
6. **Debounce UI updates**: Batch rapid updates to avoid UI thrashing

## Limitations

- **No message persistence**: Offline clients miss updates
- **No message replay**: New connections don't receive historical updates
- **No guaranteed delivery**: Network issues may lose messages
- **Single server only**: No distributed pub/sub (use Redis for multi-server)

For production deployments with multiple backend servers, consider implementing Redis pub/sub for cross-server message broadcasting.
