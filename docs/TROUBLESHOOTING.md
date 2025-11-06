# RampForge Troubleshooting Guide

This guide helps you diagnose and resolve common issues with RampForge.

---

## Table of Contents

1. [Database Issues](#database-issues)
2. [WebSocket Connection Problems](#websocket-connection-problems)
3. [Authentication Failures](#authentication-failures)
4. [Performance Issues](#performance-issues)
5. [Log Analysis](#log-analysis)
6. [Common Error Messages](#common-error-messages)

---

## Database Issues

### Issue: "No such table" error

**Symptom:**
```
sqlalchemy.exc.OperationalError: no such table: users
```

**Cause:** Database not initialized or migrations not run.

**Solution:**
```bash
cd backend
python -c "import asyncio; from app.db.session import init_db; asyncio.run(init_db())"
# Then seed the database
python app/seed.py
```

---

### Issue: Database locked (SQLite)

**Symptom:**
```
sqlite3.OperationalError: database is locked
```

**Cause:** Another process has the database file open, or previous transaction not committed.

**Solutions:**

1. **Check for other processes:**
```bash
lsof dcdock.db  # Linux/Mac
# Kill any hanging processes
```

2. **Enable WAL mode for better concurrency:**
```bash
sqlite3 dcdock.db "PRAGMA journal_mode=WAL;"
```

3. **Switch to PostgreSQL for production** (recommended for multiple connections)

---

### Issue: Connection pool exhausted (PostgreSQL)

**Symptom:**
```
TimeoutError: QueuePool limit of size 5 overflow 10 reached
```

**Cause:** Too many concurrent database connections.

**Solution:**

1. **Increase pool size** in `.env`:
```bash
DB_POOL_SIZE=10
DB_MAX_OVERFLOW=20
```

2. **Check for connection leaks:**
```python
# Ensure all database sessions are properly closed
async with get_db() as db:
    # Your code here
    pass  # Session automatically closed
```

---

### Issue: Migration fails with "column already exists"

**Symptom:**
```
sqlalchemy.exc.OperationalError: duplicate column name: direction
```

**Cause:** Migration was partially applied or run twice.

**Solution:**

Our migrations are idempotent (can be run multiple times safely). If you see this error:

1. **Check if column exists:**
```sql
PRAGMA table_info(ramps);  -- SQLite
-- or
SELECT column_name FROM information_schema.columns
WHERE table_name='ramps';  -- PostgreSQL
```

2. **Drop and recreate database** (DEVELOPMENT ONLY):
```bash
rm dcdock.db  # SQLite
python app/seed.py
```

---

## WebSocket Connection Problems

### Issue: WebSocket connection refused

**Symptom:**
```
websockets.exceptions.InvalidStatusCode: server rejected WebSocket connection: HTTP 403
```

**Cause:** Invalid or missing JWT token.

**Solutions:**

1. **Check token expiration:**
```python
# Token expires after ACCESS_TOKEN_EXPIRE_MINUTES (default: 24 hours)
# Login again to get fresh token
```

2. **Verify token format:**
```python
# Correct (as of Phase 4):
websockets.connect("ws://localhost:8000/api/ws", subprotocols=["Bearer.<token>"])

# Deprecated but still works:
websockets.connect("ws://localhost:8000/api/ws?token=<token>")
```

3. **Check backend logs** for specific auth errors

---

### Issue: WebSocket disconnects frequently

**Symptom:** Connection drops every few minutes, reconnection attempts fail.

**Causes and Solutions:**

1. **Firewall/Proxy timeout:**
```bash
# Configure proxy to allow long-lived WebSocket connections
# Nginx example:
proxy_read_timeout 600s;
proxy_send_timeout 600s;
```

2. **Network instability:**
```python
# Ensure auto-reconnect is enabled (default in Phase 4+)
ws_client = WebSocketClient(
    auto_reconnect=True,
    max_retries=5
)
```

3. **Server restart:**
```bash
# Check if backend is restarting frequently
journalctl -u dcdock-api -f  # systemd
docker logs -f dcdock-backend  # Docker
```

---

### Issue: WebSocket messages not received

**Symptom:** WebSocket connected but no updates appear in UI.

**Diagnostic steps:**

1. **Check subscription:**
```python
# Ensure you subscribed to updates
await ws_client.subscribe(direction="IB")  # or "OB" or None for all
```

2. **Verify WebSocket server logs:**
```bash
tail -f logs/dcdock.log | grep "WebSocket"
```

3. **Test with raw WebSocket:**
```python
import asyncio
import websockets
import json

async def test():
    async with websockets.connect(
        "ws://localhost:8000/api/ws",
        subprotocols=["Bearer.YOUR_TOKEN"]
    ) as ws:
        # Subscribe
        await ws.send(json.dumps({"type": "subscribe"}))

        # Wait for messages
        async for message in ws:
            print(f"Received: {message}")

asyncio.run(test())
```

---

## Authentication Failures

### Issue: "Incorrect email or password"

**Symptom:**
```json
{"detail": "Incorrect email or password"}
```

**Solutions:**

1. **Verify credentials:**
```bash
# Default demo credentials (after Phase 4):
# Admin: admin@rampforge.dev / Admin123!@#
# Operator: operator1@rampforge.dev / Operator123!@#
```

2. **Check password complexity** (Phase 4+):
- Minimum 8 characters
- At least 1 uppercase, 1 lowercase, 1 digit, 1 special character

3. **Reset password:**
```python
# In Python shell
from app.core.security import get_password_hash
from app.db.session import AsyncSessionLocal
from app.db.models import User
from sqlalchemy import select

async def reset_password():
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(User).where(User.email == "admin@rampforge.dev"))
        user = result.scalar_one()
        user.password_hash = get_password_hash("NewPassword123!")
        await db.commit()

import asyncio
asyncio.run(reset_password())
```

---

### Issue: Rate limit exceeded

**Symptom:**
```json
{"detail": "Rate limit exceeded: 5 per 1 minute"}
```

**Cause:** Too many failed login attempts (5 per minute per IP).

**Solutions:**

1. **Wait 60 seconds** and try again
2. **Check IP address** if behind proxy:
```python
# Ensure proxy forwards real client IP
# Nginx example:
proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
```

---

### Issue: "Inactive user"

**Symptom:**
```json
{"detail": "Inactive user"}
```

**Cause:** User account is disabled.

**Solution:**

Activate user in database:
```python
# Update user.is_active = True
# Only admins can do this via API:
PATCH /api/users/{user_id}
{"is_active": true}
```

---

## Performance Issues

### Issue: Slow API responses

**Diagnostic steps:**

1. **Check database query performance:**
```python
# Enable SQL logging in .env
DEBUG=true

# Look for N+1 query problems in logs
# Should see:
# SELECT * FROM assignments WHERE ...  -- Good (single query)
# Not:
# SELECT * FROM ramps WHERE id=1
# SELECT * FROM ramps WHERE id=2  -- Bad (N queries)
```

2. **Verify database indexes exist:**
```sql
-- SQLite
.schema assignments

-- PostgreSQL
SELECT indexname, indexdef FROM pg_indexes
WHERE tablename='assignments';
```

3. **Check system resources:**
```bash
# CPU and memory
top
htop

# Disk I/O
iostat -x 1

# Database connections
# PostgreSQL
SELECT count(*) FROM pg_stat_activity;
```

---

### Issue: High memory usage

**Causes:**

1. **Too many WebSocket connections:**
```python
# Check active connections
GET /api/ws/stats
# Response: {"active_connections": 150}

# Solution: Implement connection limits
```

2. **Large result sets:**
```python
# Use pagination
GET /api/assignments/?skip=0&limit=100
# Don't fetch all records at once
```

3. **Memory leak in long-running process:**
```bash
# Restart backend periodically
systemctl restart dcdock-api

# Or use process manager with automatic restarts
pm2 start backend/run.py --name dcdock-api --max-memory-restart 500M
```

---

## Log Analysis

### Log Locations

**Backend:**
- Development: `backend/logs/dcdock.log`
- Production: `/var/log/dcdock/api.log` (or configured path)

**Client TUI:**
- `~/.dcdock/client.log`

### Useful Log Commands

**Find errors:**
```bash
grep "ERROR" logs/dcdock.log | tail -50
```

**Find authentication failures:**
```bash
grep "401\|403" logs/dcdock.log
```

**Find WebSocket issues:**
```bash
grep "WebSocket" logs/dcdock.log | tail -100
```

**Find database errors:**
```bash
grep -i "database\|sql" logs/dcdock.log | grep ERROR
```

**Monitor logs in real-time:**
```bash
tail -f logs/dcdock.log
```

**Count errors by type:**
```bash
grep "ERROR" logs/dcdock.log | awk '{print $NF}' | sort | uniq -c | sort -rn
```

---

## Common Error Messages

### "Version conflict detected"

**HTTP Status:** 409 Conflict

**Meaning:** Optimistic locking detected concurrent modification.

**Solution:**
1. Fetch current data: `GET /api/assignments/{id}`
2. Retry update with new version number

**Example:**
```python
# First attempt (version=1)
PATCH /api/assignments/123
{"status_id": 2, "version": 1}
# Response: 409 Conflict

# Fetch current state
GET /api/assignments/123
# Response: {"id": 123, "version": 2, ...}

# Retry with correct version
PATCH /api/assignments/123
{"status_id": 2, "version": 2}
# Response: 200 OK
```

---

### "Email already registered"

**HTTP Status:** 400 Bad Request

**Meaning:** User with this email already exists.

**Solutions:**
1. Use different email address
2. Reset existing user's password instead
3. Delete existing user (if appropriate)

---

### "Ramp direction mismatch"

**Meaning:** Trying to assign INBOUND load to OUTBOUND ramp (or vice versa).

**Solution:** Ensure load direction matches ramp direction:
```python
# Check ramp direction
GET /api/ramps/{ramp_id}
# Response: {"direction": "INBOUND", ...}

# Check load direction
GET /api/loads/{load_id}
# Response: {"direction": "INBOUND", ...}

# They must match!
```

---

### "SECRET_KEY environment variable not set"

**Cause:** Missing SECRET_KEY in environment.

**Solution:**
```bash
# Generate secure key
python3 -c "from secrets import token_urlsafe; print(token_urlsafe(32))"

# Add to .env file
echo 'SECRET_KEY="<generated-key-here>"' >> .env

# Or export directly
export SECRET_KEY="<generated-key-here>"
```

---

## Getting Help

If you can't resolve the issue:

1. **Check logs** with maximum verbosity:
```bash
DEBUG=true python backend/run.py
```

2. **Create minimal reproduction:**
```bash
# Exact steps to reproduce the issue
1. Start fresh database
2. Run seed.py
3. Make specific API call
4. Observe error
```

3. **Gather system information:**
```bash
# Python version
python --version

# Package versions
pip list | grep -E "fastapi|sqlalchemy|websockets"

# OS information
uname -a  # Linux/Mac
systeminfo  # Windows
```

4. **Report issue** on GitHub with:
- RampForge version
- Full error message and stack trace
- Steps to reproduce
- Relevant log excerpts
- System information

---

## Preventive Maintenance

### Daily Checks

- [ ] Monitor log file size
- [ ] Check database size
- [ ] Verify API response times
- [ ] Review error counts

### Weekly Tasks

- [ ] Run database VACUUM/ANALYZE
- [ ] Review audit logs for anomalies
- [ ] Check disk space
- [ ] Update dependencies

### Monthly Tasks

- [ ] Rotate/archive old logs
- [ ] Archive old audit logs
- [ ] Review and optimize slow queries
- [ ] Test backup restoration

---

## See Also

- [Database Schema](./DATABASE_SCHEMA.md)
- [WebSocket Documentation](./WEBSOCKET.md)
- [Production Deployment](./PRODUCTION.md)
- [API Documentation](../README.md)
