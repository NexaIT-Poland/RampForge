# Production Deployment Guide

This guide covers deploying DCDock to a production environment with PostgreSQL and building portable executables.

## Table of Contents

- [PostgreSQL Setup](#postgresql-setup)
- [Backend Deployment](#backend-deployment)
- [Building Portable Executables](#building-portable-executables)
- [Docker Deployment](#docker-deployment)
- [Security Considerations](#security-considerations)
- [Monitoring](#monitoring)

---

## PostgreSQL Setup

### 1. Install PostgreSQL

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
```

**macOS (Homebrew):**
```bash
brew install postgresql@16
brew services start postgresql@16
```

**Windows:**
Download from https://www.postgresql.org/download/windows/

### 2. Create Database and User

```bash
# Connect to PostgreSQL
sudo -u postgres psql

# Create database and user
CREATE DATABASE dcdock;
CREATE USER dcdock_user WITH PASSWORD 'your_secure_password_here';
GRANT ALL PRIVILEGES ON DATABASE dcdock TO dcdock_user;

# Grant schema permissions (PostgreSQL 15+)
\c dcdock
GRANT ALL ON SCHEMA public TO dcdock_user;

# Exit
\q
```

### 3. Configure Backend for PostgreSQL

**Option 1: Environment File**

Copy the production template:
```bash
cd backend
cp .env.production .env
```

Edit `.env` and update:
```env
DATABASE_URL="postgresql+asyncpg://dcdock_user:your_secure_password@localhost:5432/dcdock"
SECRET_KEY="your-generated-secret-key"
DEBUG=false
```

**Generate a secure secret key:**
```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

**Option 2: Environment Variables**

Set environment variables directly:
```bash
export DATABASE_URL="postgresql+asyncpg://dcdock_user:password@localhost:5432/dcdock"
export SECRET_KEY="your-generated-secret-key"
export DEBUG=false
```

### 4. Initialize Database

The application will automatically create tables on startup. Seed initial data:

```bash
cd backend
python -m app.seed
```

### 5. Test Connection

```bash
cd backend
python -c "
from app.db.session import engine
import asyncio

async def test():
    async with engine.begin() as conn:
        result = await conn.execute('SELECT version()')
        print('PostgreSQL version:', result.scalar())

asyncio.run(test())
"
```

---

## Backend Deployment

### Option 1: Systemd Service (Linux)

Create `/etc/systemd/system/dcdock.service`:

```ini
[Unit]
Description=DCDock API Server
After=network.target postgresql.service

[Service]
Type=simple
User=www-data
Group=www-data
WorkingDirectory=/opt/dcdock/backend
Environment="PATH=/opt/dcdock/venv/bin"
EnvironmentFile=/opt/dcdock/backend/.env
ExecStart=/opt/dcdock/venv/bin/python run.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable dcdock
sudo systemctl start dcdock
sudo systemctl status dcdock
```

### Option 2: Docker (Recommended)

See [Docker Deployment](#docker-deployment) section below.

### Option 3: Nginx Reverse Proxy

Install Nginx:
```bash
sudo apt install nginx
```

Create `/etc/nginx/sites-available/dcdock`:
```nginx
server {
    listen 80;
    server_name api.yourdomain.com;

    location / {
        proxy_pass http://localhost:8000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # WebSocket support
    location /api/ws {
        proxy_pass http://localhost:8000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_read_timeout 86400;
    }
}
```

Enable and restart:
```bash
sudo ln -s /etc/nginx/sites-available/dcdock /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

### Option 4: SSL with Let's Encrypt

```bash
sudo apt install certbot python3-certbot-nginx
sudo certbot --nginx -d api.yourdomain.com
```

---

## Building Portable Executables

Build standalone executables for Windows and macOS that require no Python installation.

### Prerequisites

```bash
cd client_tui
pip install -e ".[build]"
```

### Build for Current Platform

**Windows:**
```bash
cd client_tui
pyinstaller dcdock.spec
```

**macOS:**
```bash
cd client_tui
pyinstaller dcdock.spec
```

**Linux:**
```bash
cd client_tui
pyinstaller dcdock.spec
```

### Output

Executables are created in `client_tui/dist/`:
- **Windows**: `dcdock.exe`
- **macOS**: `dcdock` (app bundle)
- **Linux**: `dcdock`

### Distribution

**Windows:**
1. Copy `dist/dcdock.exe` to target machine
2. Double-click to run or execute from command prompt:
   ```cmd
   dcdock.exe --api-url http://your-server:8000 --ws-url ws://your-server:8000
   ```

**macOS:**
1. Copy `dist/dcdock.app` to Applications folder
2. Run from terminal:
   ```bash
   ./dcdock.app/Contents/MacOS/dcdock --api-url http://your-server:8000
   ```

**Linux:**
1. Copy `dist/dcdock` to target machine
2. Make executable and run:
   ```bash
   chmod +x dcdock
   ./dcdock --api-url http://your-server:8000 --ws-url ws://your-server:8000
   ```

### Size Optimization

The default build includes all Python dependencies (~50-100 MB). To reduce size:

1. **Strip unused modules** - Edit `dcdock.spec` and exclude unnecessary packages
2. **Use UPX compression** (Windows/Linux):
   ```bash
   pip install pyinstaller[upx]
   pyinstaller --upx-dir=/path/to/upx dcdock.spec
   ```

---

## Docker Deployment

### Backend Container

**Dockerfile** is provided in `docker/backend/Dockerfile`.

Build and run:
```bash
cd docker

# Build backend image
docker-compose build backend

# Run with PostgreSQL
docker-compose up -d
```

Access:
- API: http://localhost:8000
- API Docs: http://localhost:8000/docs

### Full Stack with Docker Compose

The `docker-compose.yml` includes:
- PostgreSQL database
- Backend API server
- Automatic database initialization
- Volume persistence

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down

# Stop and remove data
docker-compose down -v
```

### Environment Variables

Create `docker/.env`:
```env
POSTGRES_USER=dcdock_user
POSTGRES_PASSWORD=secure_password_here
POSTGRES_DB=dcdock
SECRET_KEY=your-generated-secret-key
```

---

## Security Considerations

### 1. Secret Key

**Never use the default secret key in production!**

Generate a secure key:
```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

### 2. Database Credentials

- Use strong passwords (20+ characters, mixed case, numbers, symbols)
- Don't commit passwords to version control
- Use environment variables or secret management tools

### 3. CORS Origins

Update `CORS_ORIGINS` in `.env` to only allow your actual domains:
```env
CORS_ORIGINS=["https://yourdomain.com"]
```

### 4. HTTPS/WSS

In production, always use:
- HTTPS for API (not HTTP)
- WSS for WebSocket (not WS)

Update TUI client connection:
```bash
dcdock --api-url https://api.yourdomain.com --ws-url wss://api.yourdomain.com
```

### 5. Firewall Rules

Only expose necessary ports:
- 80/443 (HTTP/HTTPS) - Public
- 8000 (API) - Internal only (behind reverse proxy)
- 5432 (PostgreSQL) - Internal only

### 6. Database Security

```sql
-- Restrict user to specific database
REVOKE ALL ON DATABASE postgres FROM dcdock_user;

-- Use SSL for PostgreSQL connections
ALTER SYSTEM SET ssl = on;
```

### 7. Rate Limiting

Add rate limiting to prevent abuse:

```python
# In backend/app/main.py (requires slowapi)
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(429, _rate_limit_exceeded_handler)
```

---

## Monitoring

### Health Checks

**Endpoint**: `GET /health`

Returns:
```json
{"status": "healthy"}
```

Use for:
- Docker health checks
- Load balancer health probes
- Monitoring systems (Prometheus, Datadog, etc.)

### WebSocket Statistics

**Endpoint**: `GET /api/ws/stats`

Returns:
```json
{
  "active_connections": 15,
  "clients": [...]
}
```

Monitor for:
- Connection leaks
- Load distribution
- User activity

### Logging

Configure logging in production:

```python
# backend/app/core/config.py
import logging

if not settings.debug:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('/var/log/dcdock/app.log'),
            logging.StreamHandler()
        ]
    )
```

### Database Performance

Monitor PostgreSQL:
```bash
# Connection count
SELECT count(*) FROM pg_stat_activity;

# Long-running queries
SELECT pid, now() - pg_stat_activity.query_start AS duration, query
FROM pg_stat_activity
WHERE state = 'active'
ORDER BY duration DESC;

# Database size
SELECT pg_size_pretty(pg_database_size('dcdock'));
```

---

## Backup and Recovery

### Database Backup

**Automated backup script:**

```bash
#!/bin/bash
# backup-dcdock.sh

BACKUP_DIR="/backup/dcdock"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/dcdock_$DATE.sql.gz"

mkdir -p $BACKUP_DIR

# Backup database
pg_dump -U dcdock_user dcdock | gzip > $BACKUP_FILE

# Delete backups older than 30 days
find $BACKUP_DIR -name "dcdock_*.sql.gz" -mtime +30 -delete

echo "Backup completed: $BACKUP_FILE"
```

**Cron schedule** (daily at 2 AM):
```bash
crontab -e
# Add:
0 2 * * * /opt/dcdock/backup-dcdock.sh
```

### Restore from Backup

```bash
# Decompress and restore
gunzip -c /backup/dcdock/dcdock_20250101_020000.sql.gz | psql -U dcdock_user dcdock
```

---

## Performance Tuning

### PostgreSQL Configuration

Edit `/etc/postgresql/16/main/postgresql.conf`:

```ini
# Connections
max_connections = 100

# Memory
shared_buffers = 256MB
effective_cache_size = 1GB
maintenance_work_mem = 64MB
work_mem = 4MB

# Checkpoints
checkpoint_completion_target = 0.9
wal_buffers = 16MB

# Query planner
random_page_cost = 1.1
effective_io_concurrency = 200
```

Restart PostgreSQL:
```bash
sudo systemctl restart postgresql
```

### Connection Pooling

For high-traffic deployments, use PgBouncer:

```bash
sudo apt install pgbouncer
```

Configure `/etc/pgbouncer/pgbouncer.ini`:
```ini
[databases]
dcdock = host=localhost port=5432 dbname=dcdock

[pgbouncer]
listen_addr = 127.0.0.1
listen_port = 6432
auth_type = md5
auth_file = /etc/pgbouncer/userlist.txt
pool_mode = transaction
max_client_conn = 100
default_pool_size = 20
```

Update `DATABASE_URL`:
```env
DATABASE_URL="postgresql+asyncpg://dcdock_user:password@localhost:6432/dcdock"
```

---

## Scaling

### Horizontal Scaling (Multiple Backend Servers)

For multiple backend servers, use Redis for WebSocket pub/sub:

1. Install Redis
2. Update WebSocket manager to use Redis pub/sub
3. Configure load balancer with sticky sessions for WebSocket

### Load Balancer Configuration

**HAProxy example:**
```
frontend dcdock_frontend
    bind *:80
    default_backend dcdock_backend

backend dcdock_backend
    balance roundrobin
    option httpchk GET /health
    server backend1 10.0.0.1:8000 check
    server backend2 10.0.0.2:8000 check
    server backend3 10.0.0.3:8000 check
```

---

## Troubleshooting

### Cannot connect to PostgreSQL

**Check connection:**
```bash
psql -U dcdock_user -d dcdock -h localhost
```

**Common issues:**
1. `pg_hba.conf` doesn't allow connections - Add: `host dcdock dcdock_user 127.0.0.1/32 md5`
2. PostgreSQL not listening - Check `listen_addresses` in `postgresql.conf`
3. Firewall blocking port 5432 - Open port: `sudo ufw allow 5432`

### WebSocket connections failing

1. Check Nginx WebSocket configuration
2. Verify firewall allows WebSocket traffic
3. Ensure WSS (not WS) for HTTPS sites
4. Check CORS settings

### High memory usage

1. Reduce PostgreSQL `shared_buffers`
2. Limit SQLAlchemy connection pool size
3. Monitor for connection leaks
4. Check for long-running queries

### Performance issues

1. Add database indexes on frequently queried columns
2. Enable PostgreSQL query logging to identify slow queries
3. Use `EXPLAIN ANALYZE` to optimize queries
4. Consider read replicas for high read traffic

---

## Production Checklist

Before going live:

- [ ] PostgreSQL installed and configured
- [ ] Secure secret key generated
- [ ] Strong database password set
- [ ] CORS origins configured
- [ ] HTTPS/WSS enabled
- [ ] Firewall rules configured
- [ ] Backup script scheduled
- [ ] Health check endpoint tested
- [ ] Monitoring configured
- [ ] Log rotation enabled
- [ ] SSL certificate valid
- [ ] Environment variables secured
- [ ] Database migrations tested
- [ ] Load testing completed
- [ ] Disaster recovery plan documented

---

## Support

For issues or questions:
- GitHub Issues: https://github.com/TMMCx2/DCDock/issues
- Documentation: https://github.com/TMMCx2/DCDock/tree/main/docs

---

**DCDock Production Deployment Guide v0.1.0**
