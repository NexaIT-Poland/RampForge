# DCDock - Distribution Center Dock Scheduling

A lightweight desktop terminal application (TUI) for managing loading/unloading ramps in a distribution center.

## Features

- **Multi-user concurrent access** (up to 20 users)
- **Role-based access control** (Admin & Operator)
- **Real-time updates** via WebSocket
- **Optimistic locking** to prevent data conflicts
- **Audit logging** for all changes
- **Portable executable** for Windows & macOS
- **SQLite (dev) / PostgreSQL (prod)** database support

## Tech Stack

### Backend
- **FastAPI** - High-performance async API framework
- **SQLAlchemy** - Async ORM with optimistic locking
- **Pydantic** - Data validation and serialization
- **JWT** - Authentication & authorization
- **Alembic** - Database migrations
- **SQLite/PostgreSQL** - Flexible database support

### Frontend (TUI)
- **Textual** - Modern TUI framework
- **WebSocket** - Real-time board updates
- **PyInstaller** - Portable executable packaging

## Quick Start

### Prerequisites
- Python 3.12+
- pip

### Installation

```bash
# Install backend dependencies
make install-backend

# Seed database with demo data
make seed

# Run backend server
make run-backend
```

The API will be available at http://localhost:8000

### Demo Credentials

**Admin:**
- Email: `admin@dcdock.com`
- Password: `admin123`

**Operator:**
- Email: `operator1@dcdock.com`
- Password: `operator123`

## API Documentation

Once the server is running, visit:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## Project Structure

```
DCDock/
├── backend/
│   ├── app/
│   │   ├── api/          # API route handlers
│   │   ├── core/         # Core utilities (config, security)
│   │   ├── db/           # Database models and session
│   │   ├── schemas/      # Pydantic schemas
│   │   ├── services/     # Business logic
│   │   └── ws/           # WebSocket handlers (Phase 2)
│   ├── tests/            # Backend tests
│   ├── alembic/          # Database migrations
│   └── pyproject.toml    # Backend dependencies
├── client_tui/
│   ├── app/
│   │   ├── screens/      # TUI screens (Phase 3)
│   │   ├── widgets/      # Custom widgets
│   │   └── services/     # API client services
│   ├── tests/            # TUI tests
│   └── pyproject.toml    # Client dependencies
├── docs/                 # Documentation
├── Makefile              # Common tasks
└── README.md
```

## Database Schema

### Users
- Authentication and role management
- Roles: ADMIN, OPERATOR

### Ramps
- Physical loading/unloading docks
- Identified by code (R1-R8)

### Statuses
- Workflow states: PLANNED, ARRIVED, IN_PROGRESS, DELAYED, COMPLETED, CANCELLED
- Customizable colors and sort order

### Loads
- Shipments to be loaded/unloaded
- Direction: INBOUND (IB) or OUTBOUND (OB)

### Assignments
- Links ramp + load + status
- Tracks ETAs and user updates
- Optimistic locking via version field

### Audit Logs
- Complete change history
- Before/after snapshots

## Development Phases

### ✅ Phase 0 - Planning & Scaffolding
- Project structure
- Configuration files
- Tooling setup (ruff, black, mypy, pytest)

### ✅ Phase 1 - Backend (SQLite)
- Database models with optimistic locking
- FastAPI endpoints (CRUD)
- JWT authentication & RBAC
- Audit logging
- Seed data script

### ✅ Phase 2 - Real-time
- WebSocket implementation
- Live board updates
- Conflict resolution notifications
- Direction-based filtering

### 📋 Phase 3 - TUI (Textual)
- Login screen
- Main board (IB/OB/All views)
- Assignment modals
- Admin panel
- Audit viewer

### 📋 Phase 4 - Production
- PostgreSQL configuration
- PyInstaller packaging
- Windows/macOS executables

## Development Commands

```bash
# Install all dependencies
make install

# Run backend server
make run-backend

# Run tests
make test

# Lint code
make lint

# Format code
make format

# Clean generated files
make clean

# Seed database
make seed
```

## API Endpoints

### Authentication
- `POST /api/auth/login` - Login and get JWT token

### Users (Admin only)
- `GET /api/users/` - List all users
- `POST /api/users/` - Create user
- `GET /api/users/{id}` - Get user
- `PATCH /api/users/{id}` - Update user
- `DELETE /api/users/{id}` - Delete user
- `GET /api/users/me` - Get current user

### Ramps
- `GET /api/ramps/` - List all ramps
- `POST /api/ramps/` - Create ramp (Admin)
- `GET /api/ramps/{id}` - Get ramp
- `PATCH /api/ramps/{id}` - Update ramp (Admin)
- `DELETE /api/ramps/{id}` - Delete ramp (Admin)

### Statuses
- `GET /api/statuses/` - List all statuses
- `POST /api/statuses/` - Create status (Admin)
- `GET /api/statuses/{id}` - Get status
- `PATCH /api/statuses/{id}` - Update status (Admin)
- `DELETE /api/statuses/{id}` - Delete status (Admin)

### Loads
- `GET /api/loads/` - List loads (filter by direction)
- `POST /api/loads/` - Create load
- `GET /api/loads/{id}` - Get load
- `PATCH /api/loads/{id}` - Update load
- `DELETE /api/loads/{id}` - Delete load

### Assignments
- `GET /api/assignments/` - List assignments (filter by direction)
- `POST /api/assignments/` - Create assignment
- `GET /api/assignments/{id}` - Get assignment
- `PATCH /api/assignments/{id}` - Update assignment (requires version)
- `DELETE /api/assignments/{id}` - Delete assignment

### Audit Logs
- `GET /api/audit/` - List audit logs (filterable)

### WebSocket (Real-time)
- `WS /api/ws?token=JWT` - Real-time assignment updates
- `GET /api/ws/stats` - WebSocket connection statistics

## WebSocket Real-Time Updates

DCDock provides real-time updates for all assignment changes via WebSocket:

- **Instant notifications**: Assignment CREATE, UPDATE, DELETE operations
- **Conflict alerts**: Real-time notifications when optimistic locking detects conflicts
- **Direction filtering**: Subscribe to only INBOUND or OUTBOUND updates
- **Multi-client support**: 20+ concurrent connections

### Quick WebSocket Test

```bash
# Terminal 1: Start server
cd backend && python run.py

# Terminal 2: Connect WebSocket test client
python backend/test_websocket_client.py

# Terminal 3: Make changes via API
TOKEN=$(curl -s -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@dcdock.com","password":"admin123"}' | jq -r '.access_token')

curl -X POST http://localhost:8000/api/assignments/ \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"ramp_id": 1, "load_id": 1, "status_id": 2}'
```

See [docs/WEBSOCKET.md](docs/WEBSOCKET.md) for full WebSocket API documentation.

## Optimistic Locking

All updates to assignments require the current `version` number. If another user has modified the record, the update will fail with HTTP 409 Conflict, returning the current state.

Example:
```json
{
  "detail": {
    "detail": "Version conflict detected",
    "current_version": 5,
    "provided_version": 4,
    "current_data": { ... }
  }
}
```

## Environment Variables

Create a `.env` file in the `backend/` directory:

```env
# Development (SQLite)
DATABASE_URL=sqlite+aiosqlite:///./dcdock.db

# Production (PostgreSQL)
# DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/dcdock

SECRET_KEY=your-secret-key-here
ACCESS_TOKEN_EXPIRE_MINUTES=1440
DEBUG=true
```

## Testing

```bash
# Run backend tests
cd backend && pytest

# Run with coverage
cd backend && pytest --cov=app --cov-report=html
```

## License

MIT

## Contributing

This is a demonstration project. For production use, please review security settings, especially:
- Change `SECRET_KEY` to a secure random value
- Use HTTPS in production
- Configure proper CORS origins
- Use PostgreSQL for production
- Implement rate limiting
- Add monitoring and logging
