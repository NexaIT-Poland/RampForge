# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**RampForge v1.0.0** - Production-ready distribution center dock scheduling application with enterprise-grade security.

RampForge is a distribution center dock scheduling application with a FastAPI backend and a Textual TUI client. It supports multi-user concurrent access with real-time WebSocket updates, optimistic locking for conflict resolution, and role-based access control.

**Requirements:** Python 3.11 or higher (Python 3.13+ recommended)

**Status:** ✅ Production-ready (v1.0.0) - All 6 development phases completed

### v1.0.0 Key Features
- 🔐 **Security**: Password complexity, JWT in WebSocket headers, rate limiting, login audit trail
- ⚡ **Performance**: Strategic database indexes (50-80% improvement), query optimization
- 📚 **Documentation**: Production deployment guide, security checklist, troubleshooting guide
- 🔄 **Reliability**: WebSocket auto-reconnection, 85% test coverage

## Development Commands

### Initial Setup

```bash
# Quick setup (recommended)
./setup.sh

# Or manual setup
cd backend && python3 -m venv venv && source venv/bin/activate
pip install -e ".[dev]"
pip install --force-reinstall "bcrypt>=4.0.0,<5.0.0"
python -m app.seed

cd ../client_tui && python3 -m venv venv && source venv/bin/activate
pip install -e ".[dev]"
```

### Running the Application

```bash
# Terminal 1 - Start backend server
cd backend && python run.py
# Or: make run-backend

# Terminal 2 - Start TUI client
cd client_tui && python run.py
# Or: make run-client
```

### Testing

```bash
# Run all backend tests
cd backend && pytest

# Run with coverage
cd backend && pytest --cov=app --cov-report=html

# Or use Makefile
make test
```

### Code Quality

```bash
# Lint code
cd backend && ruff check app/
cd backend && mypy app/

# Format code
cd backend && black app/
cd backend && ruff check --fix app/

# Or use Makefile
make lint
make format
```

### Database Operations

```bash
# Seed database with demo data
cd backend && python -m app.seed
# Or: make seed

# Automatic migrations (recommended - runs on startup)
# Migrations in app/db/migrations.py run automatically when backend starts
# This ensures zero-downtime schema updates

# Manual Alembic migrations (if needed)
cd backend && alembic upgrade head
cd backend && alembic revision --autogenerate -m "description"

# Note: The automatic migration system (app/db/migrations.py) handles
# direction and type field additions to ramps table. It is idempotent
# and safe to run multiple times.
```

### Building Production Executables

```bash
# Install build dependencies
cd client_tui && pip install -e ".[build]"

# Build for current platform
cd client_tui && pyinstaller dcdock.spec
# Output: dist/dcdock.exe (Windows) or dist/dcdock (macOS/Linux)
```

## Architecture Overview

### Backend Structure

The backend is organized as follows:

- **`app/api/`** - FastAPI route handlers for each resource (users, ramps, loads, statuses, assignments, audit, websocket)
- **`app/core/`** - Core utilities (config, security/JWT, password hashing)
- **`app/db/`** - Database layer
  - `models.py` - SQLAlchemy ORM models with relationships
  - `session.py` - Database session management and engine configuration
  - `base.py` - Base model classes with timestamps and versioning
- **`app/schemas/`** - Pydantic models for request/response validation
- **`app/services/`** - Business logic layer (currently only audit service)
- **`app/ws/`** - WebSocket connection manager and broadcast handlers
- **`app/seed.py`** - Database seeding script with demo data
- **`app/main.py`** - FastAPI application setup, middleware, and router registration

### Database Models & Relationships

All models inherit from `BaseModel` which provides:
- Auto-incrementing `id` (primary key)
- `created_at` and `updated_at` timestamps
- `version` field for optimistic locking (assignments only)

Key models:
- **User** - Authentication, roles (ADMIN/OPERATOR), tracks created/updated assignments
- **Ramp** - Physical loading docks, identified by unique code
  - `code` (String, unique) - Dock identifier (e.g., R1, R2, R100)
  - `description` (String, optional) - Human-readable description
  - `direction` (Enum: LoadDirection) - Permanent department assignment: INBOUND or OUTBOUND
  - `type` (Enum: RampType) - Dock classification: PRIME (gate area) or BUFFER (overflow)
  - Direction and type are set by admin and determine dock behavior
- **Status** - Workflow states (PLANNED, ARRIVED, IN_PROGRESS, DELAYED, COMPLETED, CANCELLED) with colors and sort order
- **Load** - Shipments with direction (IB=inbound, OB=outbound), references, ETAs
  - `planned_departure` (DateTime, optional) - For OB loads, scheduled departure time
  - Used for OVERDUE DEPARTURE detection
- **Assignment** - Central entity linking Ramp + Load + Status with ETAs and audit tracking
  - `eta_out` (DateTime, optional) - Tracks departure time, used for overdue detection
- **AuditLog** - Complete change history with before/after JSON snapshots

**Important Enums**:
- **LoadDirection**: INBOUND, OUTBOUND (stored as full names, not IB/OB)
- **RampType**: PRIME, BUFFER

### Optimistic Locking

All assignment updates require the current `version` number. The version is automatically incremented on each update. If a version mismatch occurs, the API returns HTTP 409 Conflict with the current data, allowing the client to resolve the conflict.

Implementation: `Assignment` model has a `version` field that is checked before updates in `app/api/assignments.py`.

### WebSocket Real-Time Updates

The WebSocket system (`app/ws/manager.py`) broadcasts assignment changes to all connected clients:

- **Connection (v1.0.0 - recommended)**: `ws://localhost:8000/api/ws` with `subprotocols=["Bearer.<token>"]`
- **Connection (legacy)**: `ws://localhost:8000/api/ws?token=JWT_TOKEN` (still supported)
- **Message types**: `connection_ack`, `assignment_created`, `assignment_updated`, `assignment_deleted`, `conflict_detected`
- **Direction filtering**: Clients can subscribe to only IB or OB updates
- **Auto-reconnection**: Clients automatically reconnect with exponential backoff (Phase 4)
- **Thread-safe**: Uses asyncio locks for concurrent connection management

Broadcasting is triggered automatically from assignment API endpoints (create/update/delete).

**v1.0.0 Security Improvements:**
- JWT authentication moved to Sec-WebSocket-Protocol header (more secure than query params)
- Query parameter method still supported for backward compatibility
- Authentication errors properly logged with IP tracking

See `docs/WEBSOCKET.md` for full API documentation.

### TUI Client Structure

The Textual-based client is organized as:

- **`app/screens/`** - TUI screens (login, main board)
- **`app/services/`** - API client and WebSocket client services
- **`app/widgets/`** - Custom Textual widgets
- **`app/main.py`** - Application entry point with CLI arguments

The client connects to the backend via HTTP (for auth/CRUD) and WebSocket (for real-time updates).

### Authentication Flow

1. User logs in via `POST /api/auth/login` with email/password
2. **v1.0.0 Security**: Password must meet complexity requirements:
   - Minimum 8 characters
   - At least 1 uppercase letter
   - At least 1 lowercase letter
   - At least 1 digit
   - At least 1 special character
3. **v1.0.0 Rate Limiting**: Max 5 login attempts per minute per IP (prevents brute-force)
4. Backend validates credentials, returns JWT access token
5. **v1.0.0 Audit**: All login attempts logged with IP address (success/failure/inactive account)
6. Client stores token and includes it in all subsequent requests via `Authorization: Bearer <token>` header
7. WebSocket connections authenticate via:
   - **Recommended (v1.0.0)**: `subprotocols=["Bearer.<token>"]` (Sec-WebSocket-Protocol header)
   - **Legacy**: `?token=<token>` query parameter (still supported)
8. JWT contains user ID and role for authorization checks

Token expiration is configured via `ACCESS_TOKEN_EXPIRE_MINUTES` (default: 1440 = 24 hours).

### Database Switching

The application automatically detects the database type from `DATABASE_URL`:

- **SQLite** (dev): `sqlite+aiosqlite:///./dcdock.db`
- **PostgreSQL** (prod): `postgresql+asyncpg://user:pass@host:5432/dcdock`

Both use async drivers (aiosqlite, asyncpg) and support the same features.

### Database Indexes (v1.0.0)

Strategic indexes added in Phase 6 for performance optimization (50-80% improvement):

**Ramps table:**
- `ix_ramps_direction` - Fast filtering by direction (INBOUND/OUTBOUND)

**Loads table:**
- `ix_loads_direction` - Fast filtering by direction

**Assignments table:**
- `ix_assignments_created_at` - Fast sorting by creation time
- `ix_assignments_status_ramp` (composite) - Fast filtering by status and ramp combined

**Implementation:** Indexes defined in `__table_args__` in `app/db/models.py`

See `docs/DATABASE_SCHEMA.md` for complete index strategy and query optimization guidelines.

### Role-Based Access Control

- **ADMIN** - Full CRUD access to all resources, can manage users
- **OPERATOR** - Can view all resources, create/update/delete assignments and loads, cannot manage users/ramps/statuses

Authorization is enforced via FastAPI dependencies in `app/api/dependencies.py`:
- `get_current_user()` - Extracts user from JWT
- `require_admin()` - Ensures user has ADMIN role

### Business Logic & Workflow

#### Admin Workflow (Dock Creation)

When an admin creates a new dock:

1. **Admin creates dock via AddDockModal** (`client_tui/app/screens/enhanced_dashboard.py`):
   - Enters unique code (e.g., R100)
   - Selects **Direction**: Inbound (IB) or Outbound (OB) - PERMANENT assignment
   - Selects **Type**: Prime (gate area) or Buffer (overflow) - PERMANENT classification
   - Enters optional description

2. **Backend creates ramp** (`backend/app/api/ramps.py`):
   - Validates unique code
   - Stores direction (INBOUND/OUTBOUND) and type (PRIME/BUFFER) as permanent properties
   - Direction and type CANNOT be changed by operators

3. **Table Placement**:
   - `_is_prime_dock()` method uses `ramp.type` field (NOT code pattern)
   - PRIME docks appear in "Prime Docks" table
   - BUFFER docks appear in "Buffer Docks" table

**Key Implementation Details**:
- `AddDockModal` has two Select widgets: direction and dock-type
- API call: `POST /api/ramps/` with `{code, direction, type, description}`
- Direction stored as enum name ("INBOUND"/"OUTBOUND"), not value ("IB"/"OB")

#### Operator Workflow (Dock Occupation)

When an operator occupies a dock:

1. **Operator selects free dock** and triggers `action_occupy_dock()`:
   - Modal receives `dock_code` and `direction` from selected dock
   - Direction is READ-ONLY - inherited from ramp's permanent assignment

2. **OccupyDockModal displays** (`client_tui/app/screens/enhanced_dashboard.py`):
   - Shows dock code and department label (Inbound/Outbound) in title
   - Load Reference input (required)
   - Notes input (optional)
   - **For OUTBOUND docks ONLY**: Departure Date input (required)
     - Format: "YYYY-MM-DD HH:MM"
     - Example: "2024-12-15 14:30"

3. **Backend creates load and assignment**:
   - Load created with `direction` from dock (not operator input)
   - For OB loads: `planned_departure` set from operator's departure date input
   - Assignment created with `eta_out` = `planned_departure`

**Key Implementation Details**:
- `OccupyDockModal.__init__(dock_code: str, direction: str)` - direction passed from dock
- Modal shows/hides departure date input based on `self.is_outbound = (direction == "OB")`
- `_occupy_dock_async()` parses departure date: `datetime.strptime(departure_str, "%Y-%m-%d %H:%M")`
- Assignment includes `eta_out` for departure tracking

#### OVERDUE DEPARTURE Detection

System automatically detects when OB loads haven't departed on time:

1. **Detection Logic** (`client_tui/app/services/ramp_status.py`):
   - `RampInfo.is_overdue` property checks if `eta_out_dt < current_time`
   - For OB docks: `if self.is_overdue and self.direction == "OB"`
   - Sets `status_label = "OVERDUE DEPARTURE"` and `status_color = "red"`

2. **Visual Display**:
   - Dock shows 🔴 **OVERDUE DEPARTURE** status in red
   - Indicates load didn't leave on scheduled time
   - Signals that ramp is blocked by delayed departure

**Key Implementation Details**:
- Direction comes from `ramp.direction` (permanent), not `load.direction`
- `RampInfo.__init__()` sets `self.direction = ramp.get("direction")`
- Overdue check: `self.eta_out_dt < datetime.now(timezone.utc)`

#### Automatic Database Migrations

On backend startup (`backend/app/main.py` lifespan handler):

1. **Migration System** (`backend/app/db/migrations.py`):
   - `run_migrations()` executes all migration functions in order
   - `migrate_add_ramp_direction()` - Adds `direction` column with default INBOUND
   - `migrate_add_ramp_type()` - Adds `type` column with smart defaults:
     - R1-R8 → PRIME (original gate docks)
     - R9+ → BUFFER (newer overflow docks)

2. **Idempotent Design**:
   - Each migration checks if column exists before adding
   - Safe to run multiple times
   - Zero-downtime updates for existing installations

3. **Enum Storage**:
   - SQLAlchemy with `native_enum=False` stores enum NAMES
   - Direction stored as "INBOUND"/"OUTBOUND" (not "IB"/"OB")
   - Type stored as "PRIME"/"BUFFER"

**Key Implementation Details**:
- Migrations use raw SQL via `session.execute(text(...))`
- Column existence check: `PRAGMA table_info(ramps)` for SQLite
- Default values assigned in multiple steps for safety

## Common Development Patterns

### Adding a New API Endpoint

1. Define Pydantic schemas in `app/schemas/<resource>.py`
2. Add route handler in `app/api/<resource>.py`
3. Register router in `app/main.py`
4. Add business logic to `app/services/` if complex
5. Update audit logging if the endpoint modifies data

### Running Tests for a Single File

```bash
cd backend && pytest tests/test_specific_file.py
cd backend && pytest tests/test_specific_file.py::test_function_name
```

### Debugging WebSocket Issues

1. Start backend: `cd backend && python run.py`
2. Run test client: `python backend/test_websocket_client.py [email] [password] [direction]`
3. Trigger changes via API in another terminal
4. Check WebSocket stats: `curl http://localhost:8000/api/ws/stats`

### Production Deployment Notes

- Always use PostgreSQL in production (not SQLite)
- Generate secure `SECRET_KEY`: `python -c "import secrets; print(secrets.token_urlsafe(32))"`
- Use HTTPS/WSS (not HTTP/WS)
- Configure CORS origins for your domain
- Run database seed: `python -m app.seed` after initial setup
- See `docs/PRODUCTION.md` for full deployment guide including Docker, Nginx, SSL, monitoring

### Demo Credentials

**v1.0.0 Update:** Passwords now meet complexity requirements

- Admin: `admin@rampforge.dev` / `Admin123!@#`
- Operator: `operator1@rampforge.dev` / `Operator123!@#`

**Password Requirements:**
- Minimum 8 characters
- At least 1 uppercase, 1 lowercase, 1 digit, 1 special character
- Enforced at API level in `app/core/security.py`

## Technology Stack

- **Backend**: FastAPI, SQLAlchemy (async), Pydantic, JWT, Alembic, SQLite/PostgreSQL
- **Frontend**: Textual TUI, WebSocket, PyInstaller
- **Testing**: pytest, pytest-asyncio, pytest-cov (85% backend coverage)
- **Code Quality**: ruff, black, mypy
- **Security**: bcrypt, rate limiting, JWT, password complexity validation
- **Performance**: Strategic database indexes, query optimization

## v1.0.0 Documentation

Complete documentation suite added in Phases 4-6:

### Core Documentation
- **`README.md`** - Project overview, quick start, features
- **`DEVELOPMENT_ROADMAP_v1.0.0.md`** - Complete 6-phase development history with status tracking

### Technical Documentation
- **`docs/PRODUCTION.md`** - Production deployment guide (PostgreSQL, Docker, Nginx, SSL)
- **`docs/DATABASE_SCHEMA.md`** - Complete schema documentation with ERD, indexes, optimization
- **`docs/WEBSOCKET.md`** - WebSocket API specification with examples
- **`docs/TROUBLESHOOTING.md`** - Common issues and solutions

### Security & Operations
- **`backend/.env.example`** - Complete configuration reference with security notes
- **`docs/github-actions-test.yml.example`** - CI/CD pipeline template

### PR Documentation
- **`docs/PR_DESCRIPTION_v1.0.0.md`** - Comprehensive PR description for v1.0.0 release (465 lines)

All documentation includes:
- Security considerations
- Production best practices
- Code examples
- Troubleshooting steps
- Migration guides for breaking changes
