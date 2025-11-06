# RampForge Database Schema Documentation

## Overview

RampForge uses a relational database with support for both SQLite (development) and PostgreSQL (production). The schema is designed to manage dock assignments, track loads, monitor user actions, and maintain audit logs.

**Database Engines:**
- **Development:** SQLite with async support (`sqlite+aiosqlite`)
- **Production:** PostgreSQL with async support (`postgresql+asyncpg`)

**ORM:** SQLAlchemy 2.0+ with async support

---

## Entity Relationship Diagram

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│    User     │      │  Assignment  │      │    Ramp     │
├─────────────┤      ├──────────────┤      ├─────────────┤
│ id (PK)     │──┐   │ id (PK)      │   ┌──│ id (PK)     │
│ email       │  │   │ ramp_id (FK) │───┘  │ code        │
│ full_name   │  └──▶│ created_by   │      │ description │
│ password    │   ┌──│ updated_by   │      │ direction   │
│ role        │───┘  │ load_id (FK) │──┐   │ type        │
│ is_active   │      │ status_id(FK)│  │   └─────────────┘
│ created_at  │      │ eta_in       │  │
│ updated_at  │      │ eta_out      │  │
│ version     │      │ created_at   │  │   ┌─────────────┐
└─────────────┘      │ updated_at   │  │   │    Load     │
                     │ version      │  │   ├─────────────┤
                     └──────────────┘  └───│ id (PK)     │
                                           │ reference   │
┌─────────────┐                            │ direction   │
│   Status    │                            │ planned_arr │
├─────────────┤      ┌──────────────┐      │ planned_dep │
│ id (PK)     │◀─────│  Assignment  │      │ notes       │
│ code        │      └──────────────┘      │ created_at  │
│ label       │                            │ updated_at  │
│ color       │                            │ version     │
│ sort_order  │                            └─────────────┘
└─────────────┘
                     ┌──────────────┐
                     │  AuditLog    │
                     ├──────────────┤
                     │ id (PK)      │
                     │ entity_type  │
                     │ entity_id    │
                     │ action       │
                     │ user_id (FK) │
                     │ before       │
                     │ after        │
                     │ timestamp    │
                     └──────────────┘
```

---

## Tables

### 1. `users`

Stores user accounts with authentication and authorization data.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | INTEGER | PRIMARY KEY | Auto-incrementing user ID |
| `email` | VARCHAR(255) | UNIQUE, NOT NULL | User's email address (unique identifier) |
| `full_name` | VARCHAR(255) | NOT NULL | Full name of the user |
| `password_hash` | VARCHAR(255) | NOT NULL | Bcrypt hashed password |
| `role` | ENUM | NOT NULL | User role: `ADMIN` or `OPERATOR` |
| `is_active` | BOOLEAN | DEFAULT TRUE | Account status (active/inactive) |
| `created_at` | TIMESTAMP | DEFAULT NOW | Account creation timestamp |
| `updated_at` | TIMESTAMP | DEFAULT NOW | Last update timestamp |
| `version` | INTEGER | DEFAULT 1 | Version for optimistic locking |

**Indexes:**
- `ix_users_email` (UNIQUE) - Fast email lookups for authentication

**Password Requirements (as of Phase 4):**
- Minimum 8 characters
- At least 1 uppercase letter
- At least 1 lowercase letter
- At least 1 digit
- At least 1 special character

**Roles:**
- `ADMIN`: Full access (create users, manage docks, view audit logs)
- `OPERATOR`: Limited access (manage assignments, view docks)

---

### 2. `ramps`

Stores dock/ramp information for loading and unloading operations.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | INTEGER | PRIMARY KEY | Auto-incrementing ramp ID |
| `code` | VARCHAR(50) | UNIQUE, NOT NULL | Ramp identifier (e.g., "R1", "R2") |
| `description` | TEXT | NULLABLE | Human-readable description of the ramp |
| `direction` | ENUM | NOT NULL | Load direction: `INBOUND` or `OUTBOUND` |
| `type` | ENUM | NOT NULL | Ramp type: `PRIME` or `BUFFER` |
| `created_at` | TIMESTAMP | DEFAULT NOW | Creation timestamp |
| `updated_at` | TIMESTAMP | DEFAULT NOW | Last update timestamp |
| `version` | INTEGER | DEFAULT 1 | Version for optimistic locking |

**Indexes:**
- `ix_ramps_code` (UNIQUE) - Fast ramp code lookups
- `ix_ramps_direction` - Fast filtering by direction

**Direction Types:**
- `INBOUND` (IB): Receiving goods (incoming deliveries)
- `OUTBOUND` (OB): Shipping goods (outgoing deliveries)

**Ramp Types:**
- `PRIME`: Main gate area ramps (preferred for operations)
- `BUFFER`: Overflow area ramps (used when prime ramps are full)

---

### 3. `loads`

Stores information about shipments/loads to be processed.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | INTEGER | PRIMARY KEY | Auto-incrementing load ID |
| `reference` | VARCHAR(100) | UNIQUE, NOT NULL | Load reference number (e.g., "IB-2024-001") |
| `direction` | ENUM | NOT NULL | Load direction: `INBOUND` or `OUTBOUND` |
| `planned_arrival` | TIMESTAMP | NULLABLE | Expected arrival time at dock |
| `planned_departure` | TIMESTAMP | NULLABLE | Expected departure time from dock |
| `notes` | TEXT | NULLABLE | Additional notes about the load |
| `created_at` | TIMESTAMP | DEFAULT NOW | Creation timestamp |
| `updated_at` | TIMESTAMP | DEFAULT NOW | Last update timestamp |
| `version` | INTEGER | DEFAULT 1 | Version for optimistic locking |

**Indexes:**
- `ix_loads_reference` (UNIQUE) - Fast reference lookups
- `ix_loads_direction` - Fast filtering by direction

**Business Rules:**
- Inbound loads: `planned_arrival` is required
- Outbound loads: `planned_departure` is required
- Reference format convention: `{DIRECTION}-{YEAR}-{NUMBER}`

---

### 4. `statuses`

Stores predefined statuses for assignment workflow.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | INTEGER | PRIMARY KEY | Auto-incrementing status ID |
| `code` | VARCHAR(50) | UNIQUE, NOT NULL | Status code (e.g., "PLANNED", "IN_PROGRESS") |
| `label` | VARCHAR(100) | NOT NULL | Display label for UI |
| `color` | VARCHAR(50) | NOT NULL | UI color hint (e.g., "blue", "green") |
| `sort_order` | INTEGER | DEFAULT 0 | Display order in UI |
| `created_at` | TIMESTAMP | DEFAULT NOW | Creation timestamp |
| `updated_at` | TIMESTAMP | DEFAULT NOW | Last update timestamp |

**Indexes:**
- `ix_statuses_code` (UNIQUE) - Fast code lookups

**Standard Statuses:**
1. `PLANNED` (blue) - Assignment scheduled but not started
2. `ARRIVED` (cyan) - Load has arrived at dock
3. `IN_PROGRESS` (yellow) - Loading/unloading in progress
4. `DELAYED` (orange) - Operation delayed
5. `COMPLETED` (green) - Operation completed successfully
6. `CANCELLED` (red) - Operation cancelled

---

### 5. `assignments`

Core table storing dock-to-load assignments with optimistic locking for concurrency control.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | INTEGER | PRIMARY KEY | Auto-incrementing assignment ID |
| `ramp_id` | INTEGER | FK → ramps.id, NOT NULL | Assigned ramp/dock |
| `load_id` | INTEGER | FK → loads.id, NOT NULL | Assigned load |
| `status_id` | INTEGER | FK → statuses.id, NOT NULL | Current assignment status |
| `eta_in` | TIMESTAMP | NULLABLE | Estimated time of arrival at dock |
| `eta_out` | TIMESTAMP | NULLABLE | Estimated time of departure from dock |
| `created_by` | INTEGER | FK → users.id, NOT NULL | User who created the assignment |
| `updated_by` | INTEGER | FK → users.id, NOT NULL | User who last updated the assignment |
| `created_at` | TIMESTAMP | DEFAULT NOW | Creation timestamp |
| `updated_at` | TIMESTAMP | DEFAULT NOW | Last update timestamp |
| `version` | INTEGER | DEFAULT 1 | **Version for optimistic locking** |

**Indexes:**
- `ix_assignments_ramp_id` - Fast filtering by ramp
- `ix_assignments_load_id` - Fast filtering by load
- `ix_assignments_status_id` - Fast filtering by status
- `ix_assignments_created_at` - Fast sorting by creation time

**Foreign Key Constraints:**
- `ramp_id` references `ramps(id)` ON DELETE RESTRICT
- `load_id` references `loads(id)` ON DELETE RESTRICT
- `status_id` references `statuses(id)` ON DELETE RESTRICT
- `created_by` references `users(id)` ON DELETE RESTRICT
- `updated_by` references `users(id)` ON DELETE RESTRICT

**Optimistic Locking:**
- Every UPDATE must include current `version` in WHERE clause
- Version is automatically incremented on successful update
- Concurrent updates are detected and rejected with HTTP 409 Conflict

**Business Rules:**
- One load can only be assigned to one ramp at a time
- Ramp direction must match load direction (enforced at application level)
- ETA times should be within planned arrival/departure window

---

### 6. `audit_logs`

Stores complete audit trail of all data changes for compliance and debugging.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | INTEGER | PRIMARY KEY | Auto-incrementing log ID |
| `entity_type` | VARCHAR(50) | NOT NULL | Type of entity changed (e.g., "assignment", "user") |
| `entity_id` | INTEGER | NOT NULL | ID of the changed entity |
| `action` | VARCHAR(20) | NOT NULL | Action performed: `CREATE`, `UPDATE`, `DELETE` |
| `user_id` | INTEGER | FK → users.id, NULLABLE | User who performed the action |
| `before_data` | JSON | NULLABLE | Entity state before change (JSON) |
| `after_data` | JSON | NULLABLE | Entity state after change (JSON) |
| `timestamp` | TIMESTAMP | DEFAULT NOW | When the action occurred |

**Indexes:**
- `ix_audit_logs_entity` (entity_type, entity_id) - Fast entity history lookup
- `ix_audit_logs_user_id` - Fast user activity lookup
- `ix_audit_logs_timestamp` - Fast time-range queries

**Usage:**
- Automatically populated by `AuditService` on CRUD operations
- JSON fields contain complete snapshots for rollback capability
- Immutable - audit logs are never updated or deleted
- Used for compliance, debugging, and conflict resolution

---

## Migrations

### Current Migrations

**Migration 001: Add direction column to ramps**
- Added `direction` ENUM column (INBOUND/OUTBOUND)
- Set default value for existing records
- Made column NOT NULL after backfill

**Migration 002: Add type column to ramps**
- Added `type` ENUM column (PRIME/BUFFER)
- Set default value based on ramp code pattern
- Made column NOT NULL after backfill

**Migration 003: Add direction column to loads**
- Added `direction` ENUM column (INBOUND/OUTBOUND)
- Extracted direction from reference number
- Made column NOT NULL after backfill

### Migration Strategy

1. **Development:** SQLite migrations use `ALTER TABLE` with temporary tables
2. **Production:** PostgreSQL migrations use `ALTER TABLE` with default values
3. **Idempotent:** All migrations check for column existence before applying
4. **Backward Compatible:** New columns added with defaults before making NOT NULL

---

## Indexes Strategy

### Performance Indexes

1. **Foreign Keys** - Automatically indexed for JOIN performance
2. **Unique Constraints** - Automatically indexed for uniqueness checks
3. **Filter Columns** - `direction`, `status_id` indexed for WHERE clauses
4. **Sort Columns** - `created_at`, `sort_order` indexed for ORDER BY
5. **Search Columns** - `email`, `code`, `reference` indexed for lookups

### Index Naming Convention

- `ix_{table}_{column}` - Single column index
- `ix_{table}_{col1}_{col2}` - Composite index
- `uq_{table}_{column}` - Unique constraint
- `fk_{table}_{column}` - Foreign key

---

## Query Optimization

### Recommended Query Patterns

**1. Get All Assignments for a Ramp:**
```sql
SELECT * FROM assignments
WHERE ramp_id = ?
ORDER BY created_at DESC;
-- Uses: ix_assignments_ramp_id + ix_assignments_created_at
```

**2. Get Assignments by Direction:**
```sql
SELECT a.*, r.code, l.reference
FROM assignments a
JOIN ramps r ON a.ramp_id = r.id
JOIN loads l ON a.load_id = l.id
WHERE r.direction = 'INBOUND'
AND a.status_id IN (1, 2, 3);
-- Uses: ix_ramps_direction + ix_assignments_status_id
```

**3. Get User Activity:**
```sql
SELECT * FROM audit_logs
WHERE user_id = ?
AND timestamp > NOW() - INTERVAL '7 days'
ORDER BY timestamp DESC;
-- Uses: ix_audit_logs_user_id + ix_audit_logs_timestamp
```

---

## Backup and Maintenance

### SQLite (Development)

```bash
# Backup
sqlite3 dcdock.db ".backup dcdock_backup.db"

# Vacuum (reclaim space)
sqlite3 dcdock.db "VACUUM;"

# Analyze (update statistics)
sqlite3 dcdock.db "ANALYZE;"
```

### PostgreSQL (Production)

```bash
# Backup
pg_dump -U dcdock -h localhost dcdock > dcdock_backup.sql

# Vacuum (maintenance)
psql -U dcdock -d dcdock -c "VACUUM ANALYZE;"

# Reindex
psql -U dcdock -d dcdock -c "REINDEX DATABASE dcdock;"
```

### Recommended Schedule

- **Daily:** Automated backups
- **Weekly:** VACUUM ANALYZE
- **Monthly:** REINDEX
- **Quarterly:** Review and archive old audit logs

---

## Security Considerations

1. **Password Storage:** Bcrypt with automatic salt generation
2. **SQL Injection:** Parameterized queries via SQLAlchemy ORM
3. **Access Control:** Role-based permissions enforced at API level
4. **Audit Trail:** Complete change history for all sensitive operations
5. **Connection Pooling:** Limited concurrent connections to prevent overload

---

## See Also

- [API Documentation](../README.md)
- [WebSocket Documentation](./WEBSOCKET.md)
- [Production Deployment](./PRODUCTION.md)
- [Troubleshooting Guide](./TROUBLESHOOTING.md)
