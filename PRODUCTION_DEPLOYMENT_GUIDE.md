# RampForge Production Deployment Guide

**Dla początkujących** - Kompletny przewodnik wdrożenia produkcyjnego RampForge v1.0.0

## 📑 Spis Treści

1. [Przygotowanie VPS](#1-przygotowanie-vps)
2. [Instalacja wymagań systemowych](#2-instalacja-wymagań-systemowych)
3. [Konfiguracja PostgreSQL](#3-konfiguracja-postgresql)
4. [Deploy backendu](#4-deploy-backendu)
5. [Konfiguracja Nginx](#5-konfiguracja-nginx)
6. [SSL/TLS z Let's Encrypt](#6-ssltls-z-lets-encrypt)
7. [Systemd Service (auto-restart)](#7-systemd-service)
8. [Instalacja klienta dla operatorów](#8-instalacja-klienta-dla-operatorów)
9. [Bezpieczeństwo i monitoring](#9-bezpieczeństwo-i-monitoring)
10. [Backup i recovery](#10-backup-i-recovery)

---

## 1. Przygotowanie VPS

### 1.1 Wybór providera VPS

**Rekomendowane opcje (cena/miesiąc):**
- **DigitalOcean** - Droplet 2GB RAM ($12/miesiąc) ⭐ Najłatwiejszy dla początkujących
- **Hetzner** - CX21 (2 vCPU, 4GB RAM, €5.39/miesiąc) ⭐ Najlepsza cena/wydajność
- **Vultr** - Cloud Compute 2GB RAM ($12/miesiąc)
- **OVH** - VPS Starter (2GB RAM, ~€5/miesiąc)

**Minimalne wymagania:**
- 2GB RAM
- 2 CPU
- 20GB SSD
- Ubuntu 22.04 LTS lub 24.04 LTS

### 1.2 Domena (opcjonalnie, ale zalecane)

**Gdzie kupić domenę:**
- Cloudflare (~$10/rok dla .com)
- Namecheap
- OVH

**Konfiguracja DNS:**
```
Typ: A
Host: @ (lub dcdock)
Wartość: [IP Twojego VPS]
TTL: 300
```

Jeśli masz domenę `example.com`, backend będzie dostępny pod:
- `https://dcdock.example.com` (API)
- `wss://dcdock.example.com/api/v1/ws` (WebSocket)

**Bez domeny:** Możesz użyć bezpośrednio IP VPS, ale nie będzie SSL (lub trzeba self-signed cert).

### 1.3 Pierwsze logowanie do VPS

```bash
# Połącz się przez SSH (hasło dostaniesz od providera)
ssh root@YOUR_VPS_IP

# Utwórz użytkownika (nie pracuj jako root!)
adduser dcdock
usermod -aG sudo dcdock

# Ustaw hasło
passwd dcdock

# Przejdź na nowego użytkownika
su - dcdock
```

---

## 2. Instalacja wymagań systemowych

```bash
# Aktualizacja systemu
sudo apt update && sudo apt upgrade -y

# Podstawowe narzędzia
sudo apt install -y git curl wget nano ufw

# Python 3.11+ (Ubuntu 22.04 może wymagać PPA)
sudo apt install -y python3 python3-pip python3-venv

# Sprawdź wersję Python (musi być 3.11+)
python3 --version

# Jeśli Python < 3.11, dodaj PPA:
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install -y python3.11 python3.11-venv python3.11-dev

# PostgreSQL
sudo apt install -y postgresql postgresql-contrib

# Nginx
sudo apt install -y nginx

# Certbot (dla SSL)
sudo apt install -y certbot python3-certbot-nginx
```

---

## 3. Konfiguracja PostgreSQL

### 3.1 Utwórz bazę danych produkcyjną

```bash
# Przełącz się na użytkownika postgres
sudo -u postgres psql

# W PostgreSQL prompt:
CREATE DATABASE dcdock_prod;
CREATE USER dcdock_user WITH PASSWORD 'TWOJE_SUPER_BEZPIECZNE_HASLO';
GRANT ALL PRIVILEGES ON DATABASE dcdock_prod TO dcdock_user;

# PostgreSQL 15+ wymaga dodatkowegoGrantu:
\c dcdock_prod
GRANT ALL ON SCHEMA public TO dcdock_user;

# Wyjdź
\q
```

### 3.2 Konfiguracja bezpieczeństwa PostgreSQL

```bash
# Edytuj pg_hba.conf
sudo nano /etc/postgresql/14/main/pg_hba.conf

# Zmień linię:
# local   all             all                                     peer
# NA:
local   all             all                                     md5

# Restart PostgreSQL
sudo systemctl restart postgresql

# Testuj połączenie
psql -U dcdock_user -d dcdock_prod -h localhost
# Wprowadź hasło
# Jeśli działa, wpisz: \q
```

---

## 4. Deploy backendu

### 4.1 Clone repozytorium

```bash
cd /home/dcdock
git clone https://github.com/TMMCx2/RampForge.git
cd RampForge
```

### 4.2 Konfiguracja środowiska produkcyjnego

```bash
cd backend

# Utwórz plik .env.production
nano .env.production
```

**Zawartość `.env.production`:**

```env
# Database
DATABASE_URL=postgresql://dcdock_user:TWOJE_SUPER_BEZPIECZNE_HASLO@localhost:5432/dcdock_prod

# Security
SECRET_KEY=WYGENERUJ_BARDZO_DŁUGI_LOSOWY_STRING_64_ZNAKI_MINIMUM
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=480

# Environment
ENVIRONMENT=production

# CORS (jeśli będziesz mieć web frontend w przyszłości)
BACKEND_CORS_ORIGINS=["https://dcdock.example.com"]

# Server
HOST=0.0.0.0
PORT=8000

# Logging
LOG_LEVEL=INFO
```

**Generowanie SECRET_KEY:**
```bash
python3 -c "import secrets; print(secrets.token_urlsafe(64))"
```

### 4.3 Setup środowiska Python

```bash
# Utwórz virtual environment
python3 -m venv venv

# Aktywuj venv
source venv/bin/activate

# Zainstaluj zależności
pip install --upgrade pip
pip install -r requirements.txt

# WAŻNE: Zainstaluj Gunicorn (production ASGI server)
pip install gunicorn uvicorn[standard]
```

### 4.4 Inicjalizacja bazy danych

```bash
# Załaduj zmienne środowiskowe
export $(cat .env.production | xargs)

# Uruchom migracje (seed)
python3 -m app.seed

# Powinno stworzyć:
# - Tabele
# - Statusy
# - Użytkowników demo (admin, operator1, operator2)
# - Rampy demo
```

### 4.5 Testowe uruchomienie

```bash
# Test lokalnie
python3 run.py

# W innym terminalu (SSH session 2):
curl http://localhost:8000/api/v1/health

# Jeśli działa, zatrzymaj (Ctrl+C)
```

---

## 5. Konfiguracja Nginx

### 5.1 Utwórz konfigurację Nginx

```bash
sudo nano /etc/nginx/sites-available/dcdock
```

**Zawartość (jeśli masz domenę):**

```nginx
# Upstream dla backendu
upstream dcdock_backend {
    server 127.0.0.1:8000;
}

# HTTP -> HTTPS redirect
server {
    listen 80;
    server_name dcdock.example.com;
    return 301 https://$server_name$request_uri;
}

# HTTPS Server
server {
    listen 443 ssl http2;
    server_name dcdock.example.com;

    # SSL certificates (będą dodane przez certbot)
    ssl_certificate /etc/letsencrypt/live/dcdock.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/dcdock.example.com/privkey.pem;

    # SSL config
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_prefer_server_ciphers on;
    ssl_ciphers ECDHE-RSA-AES256-GCM-SHA512:DHE-RSA-AES256-GCM-SHA512:ECDHE-RSA-AES256-GCM-SHA384:DHE-RSA-AES256-GCM-SHA384;

    # Security headers
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header X-Content-Type-Options "nosniff" always;

    # Client body size (dla upload plików w przyszłości)
    client_max_body_size 10M;

    # API endpoints
    location /api/ {
        proxy_pass http://dcdock_backend;
        proxy_http_version 1.1;

        # Headers
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Timeouts
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }

    # WebSocket endpoint
    location /api/v1/ws {
        proxy_pass http://dcdock_backend;
        proxy_http_version 1.1;

        # WebSocket headers
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        # Standard headers
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Timeouts dla długo żyjących połączeń
        proxy_connect_timeout 7d;
        proxy_send_timeout 7d;
        proxy_read_timeout 7d;
    }

    # Health check
    location /health {
        proxy_pass http://dcdock_backend/api/v1/health;
        access_log off;
    }

    # Docs (opcjonalnie, możesz wyłączyć w prod)
    location /docs {
        proxy_pass http://dcdock_backend/docs;
    }

    location /redoc {
        proxy_pass http://dcdock_backend/redoc;
    }
}
```

**Jeśli NIE masz domeny (tylko IP):**

```nginx
upstream dcdock_backend {
    server 127.0.0.1:8000;
}

server {
    listen 80;
    server_name YOUR_VPS_IP;

    location /api/ {
        proxy_pass http://dcdock_backend;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }

    location /api/v1/ws {
        proxy_pass http://dcdock_backend;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_connect_timeout 7d;
        proxy_send_timeout 7d;
        proxy_read_timeout 7d;
    }
}
```

### 5.2 Aktywuj konfigurację

```bash
# Symlink do sites-enabled
sudo ln -s /etc/nginx/sites-available/dcdock /etc/nginx/sites-enabled/

# Usuń domyślną konfigurację
sudo rm /etc/nginx/sites-enabled/default

# Test konfiguracji
sudo nginx -t

# Jeśli OK, restart Nginx
sudo systemctl restart nginx
```

---

## 6. SSL/TLS z Let's Encrypt

**⚠️ WAŻNE:** Wymaga domeny! Jeśli nie masz domeny, pomiń ten krok.

```bash
# Zatrzymaj Nginx tymczasowo
sudo systemctl stop nginx

# Uzyskaj certyfikat
sudo certbot certonly --standalone -d dcdock.example.com

# Odpowiedz na pytania:
# - Email: office@nexait.pl
# - Agree to ToS: Yes

# Uruchom Nginx z powrotem
sudo systemctl start nginx

# Test auto-renewal
sudo certbot renew --dry-run

# Certbot automatycznie odnowi certyfikat przed wygaśnięciem (co 90 dni)
```

---

## 7. Systemd Service (auto-restart)

### 7.1 Utwórz service file

```bash
sudo nano /etc/systemd/system/dcdock.service
```

**Zawartość:**

```ini
[Unit]
Description=RampForge FastAPI Backend
After=network.target postgresql.service

[Service]
Type=notify
User=dcdock
Group=dcdock
WorkingDirectory=/home/dcdock/RampForge/backend
Environment="PATH=/home/dcdock/RampForge/backend/venv/bin"
EnvironmentFile=/home/dcdock/RampForge/backend/.env.production

# Gunicorn z Uvicorn workers (production-ready)
ExecStart=/home/dcdock/RampForge/backend/venv/bin/gunicorn \
    --workers 4 \
    --worker-class uvicorn.workers.UvicornWorker \
    --bind 0.0.0.0:8000 \
    --timeout 120 \
    --access-logfile /var/log/dcdock/access.log \
    --error-logfile /var/log/dcdock/error.log \
    --log-level info \
    app.main:app

# Restart policy
Restart=always
RestartSec=5

# Security
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

### 7.2 Utwórz katalog logów

```bash
sudo mkdir -p /var/log/dcdock
sudo chown dcdock:dcdock /var/log/dcdock
```

### 7.3 Aktywuj service

```bash
# Reload systemd
sudo systemctl daemon-reload

# Włącz autostart
sudo systemctl enable dcdock

# Uruchom service
sudo systemctl start dcdock

# Sprawdź status
sudo systemctl status dcdock

# Logi
sudo journalctl -u dcdock -f
```

**Jeśli wszystko działa, zobaczysz:**
```
● dcdock.service - RampForge FastAPI Backend
     Loaded: loaded
     Active: active (running)
```

### 7.4 Test połączenia

```bash
# Test z VPS
curl http://localhost:8000/api/v1/health

# Test z zewnątrz (z Twojego komputera)
curl https://dcdock.example.com/api/v1/health

# Powinno zwrócić:
{"status": "healthy"}
```

---

## 8. Instalacja klienta dla operatorów

Każdy operator musi zainstalować klienta TUI na swoim komputerze (Windows/Mac/Linux).

### 8.1 Dla Windows (PowerShell)

```powershell
# 1. Zainstaluj Python 3.11+ z python.org
# 2. Otwórz PowerShell i:

# Clone repo (lub pobierz ZIP i rozpakuj)
git clone https://github.com/TMMCx2/RampForge.git
cd RampForge\client_tui

# Utwórz venv
python -m venv venv
venv\Scripts\activate

# Zainstaluj zależności
pip install -r requirements.txt

# Skonfiguruj connection
notepad config.yaml
```

**config.yaml:**
```yaml
api:
  base_url: "https://dcdock.example.com/api/v1"
  websocket_url: "wss://dcdock.example.com/api/v1/ws"
  timeout: 30

logging:
  level: "INFO"
  file: "dcdock_client.log"
```

**Uruchom klienta:**
```powershell
python -m app.main
```

### 8.2 Dla Linux/Mac

```bash
# Clone repo
git clone https://github.com/TMMCx2/RampForge.git
cd RampForge/client_tui

# Setup
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Config
nano config.yaml
# (jak wyżej)

# Uruchom
python3 -m app.main
```

### 8.3 Utwórz skrypt startowy dla operatorów

**Windows (start_client.bat):**
```batch
@echo off
cd /d "%~dp0"
call venv\Scripts\activate
python -m app.main
pause
```

**Linux/Mac (start_client.sh):**
```bash
#!/bin/bash
cd "$(dirname "$0")"
source venv/bin/activate
python3 -m app.main
```

### 8.4 Dokumentacja dla operatorów

Stwórz prosty dokument:

```markdown
# RampForge Client - Instrukcja

## Pierwsze uruchomienie:
1. Kliknij dwukrotnie `start_client.bat` (Windows) lub `start_client.sh` (Mac/Linux)
2. Zaloguj się:
   - Email: [Twój email operatora]
   - Hasło: [Twoje hasło]

## Skróty klawiszowe:
- [R] - Refresh
- [O] - Occupy dock
- [F] - Free dock
- [B] - Block dock
- [S] - Toggle sort
- [1][2][3] - Filtry (All/IB/OB)
- [Ctrl+F] - Search
- [ESC] - Logout

## Wsparcie:
Email: office@nexait.pl
```

---

## 9. Bezpieczeństwo i monitoring

### 9.1 Firewall (UFW)

```bash
# Włącz UFW
sudo ufw enable

# Allow SSH
sudo ufw allow 22/tcp

# Allow HTTP i HTTPS
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp

# Sprawdź status
sudo ufw status

# PostgreSQL NIE POWINIEN być dostępny z zewnątrz (tylko localhost)
```

### 9.2 Rate limiting w Nginx

Dodaj do `/etc/nginx/nginx.conf` (w sekcji `http`):

```nginx
# Rate limiting
limit_req_zone $binary_remote_addr zone=api_limit:10m rate=10r/s;
limit_req_zone $binary_remote_addr zone=login_limit:10m rate=5r/m;
```

W `/etc/nginx/sites-available/dcdock` dodaj do lokalizacji `/api/v1/auth/login`:

```nginx
location /api/v1/auth/login {
    limit_req zone=login_limit burst=5 nodelay;
    proxy_pass http://dcdock_backend;
    # ... reszta proxy headers
}

location /api/ {
    limit_req zone=api_limit burst=20 nodelay;
    # ... reszta konfiguracji
}
```

### 9.3 Fail2Ban (ochrona przed brute-force)

```bash
# Zainstaluj Fail2Ban
sudo apt install -y fail2ban

# Utwórz konfigurację dla RampForge
sudo nano /etc/fail2ban/jail.local
```

**Zawartość:**
```ini
[dcdock-auth]
enabled = true
port = http,https
filter = dcdock-auth
logpath = /var/log/dcdock/access.log
maxretry = 5
bantime = 3600
findtime = 600
```

Utwórz filter:
```bash
sudo nano /etc/fail2ban/filter.d/dcdock-auth.conf
```

```ini
[Definition]
failregex = ^.*"POST /api/v1/auth/login HTTP.*" 401.*$
ignoreregex =
```

Restart Fail2Ban:
```bash
sudo systemctl restart fail2ban
sudo fail2ban-client status dcdock-auth
```

### 9.4 Monitoring

**Podstawowy monitoring:**

```bash
# Utwórz skrypt monitorujący
nano /home/dcdock/monitor_dcdock.sh
```

```bash
#!/bin/bash

# Check if service is running
if ! systemctl is-active --quiet dcdock; then
    echo "RampForge service is DOWN! Restarting..."
    systemctl restart dcdock
    echo "RampForge service restarted at $(date)" >> /var/log/dcdock/monitor.log
fi

# Check if backend responds
if ! curl -f -s http://localhost:8000/api/v1/health > /dev/null; then
    echo "RampForge API not responding! Restarting..."
    systemctl restart dcdock
    echo "RampForge API restart at $(date)" >> /var/log/dcdock/monitor.log
fi
```

Dodaj do crontab:
```bash
chmod +x /home/dcdock/monitor_dcdock.sh
crontab -e

# Dodaj linię (check co 5 minut):
*/5 * * * * /home/dcdock/monitor_dcdock.sh
```

---

## 10. Backup i Recovery

### 10.1 Automatyczny backup bazy danych

```bash
# Utwórz katalog backupów
sudo mkdir -p /var/backups/dcdock
sudo chown dcdock:dcdock /var/backups/dcdock

# Utwórz skrypt backup
nano /home/dcdock/backup_db.sh
```

**Zawartość:**
```bash
#!/bin/bash

# Backup settings
BACKUP_DIR="/var/backups/dcdock"
DB_NAME="dcdock_prod"
DB_USER="dcdock_user"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_FILE="$BACKUP_DIR/dcdock_backup_$TIMESTAMP.sql.gz"

# Export password (dla automatyzacji)
export PGPASSWORD='TWOJE_HASLO_DO_BAZY'

# Create backup
pg_dump -U $DB_USER -h localhost $DB_NAME | gzip > $BACKUP_FILE

# Keep only last 30 days
find $BACKUP_DIR -name "dcdock_backup_*.sql.gz" -mtime +30 -delete

# Unset password
unset PGPASSWORD

echo "Backup created: $BACKUP_FILE"
```

```bash
chmod +x /home/dcdock/backup_db.sh

# Test
/home/dcdock/backup_db.sh

# Dodaj do crontab (backup codziennie o 2:00 AM)
crontab -e
# Dodaj:
0 2 * * * /home/dcdock/backup_db.sh >> /var/log/dcdock/backup.log 2>&1
```

### 10.2 Restore z backupu

```bash
# Zatrzymaj backend
sudo systemctl stop dcdock

# Restore
export PGPASSWORD='TWOJE_HASLO_DO_BAZY'
gunzip -c /var/backups/dcdock/dcdock_backup_TIMESTAMP.sql.gz | \
    psql -U dcdock_user -h localhost dcdock_prod
unset PGPASSWORD

# Uruchom backend
sudo systemctl start dcdock
```

---

## 🎯 Checklist finalna

Po zakończeniu wszystkich kroków, sprawdź:

- [ ] Backend działa: `curl https://dcdock.example.com/api/v1/health`
- [ ] WebSocket działa: Połącz klienta TUI i zobacz real-time updates
- [ ] SSL certyfikat ważny: Sprawdź w przeglądarce (zielona kłódka)
- [ ] Firewall skonfigurowany: `sudo ufw status`
- [ ] Systemd service enabled: `sudo systemctl is-enabled dcdock`
- [ ] Backup działa: Sprawdź `/var/backups/dcdock/`
- [ ] Monitoring działa: Sprawdź logi `/var/log/dcdock/`
- [ ] Rate limiting działa: Testuj wiele requestów
- [ ] Dokumentacja dla operatorów gotowa

---

## 📞 Troubleshooting

### Backend nie startuje

```bash
# Check logi
sudo journalctl -u dcdock -n 50

# Najczęstsze problemy:
# 1. Błąd połączenia z bazą - sprawdź DATABASE_URL w .env.production
# 2. Port zajęty - sprawdź: sudo lsof -i :8000
# 3. Permission errors - sprawdź: ls -la /home/dcdock/RampForge/backend/
```

### WebSocket nie działa

```bash
# Check Nginx logs
sudo tail -f /var/log/nginx/error.log

# Sprawdź konfigurację WebSocket w Nginx
sudo nginx -t
```

### Baza danych nie odpowiada

```bash
# Check PostgreSQL status
sudo systemctl status postgresql

# Check connections
sudo -u postgres psql -c "SELECT * FROM pg_stat_activity;"

# Restart PostgreSQL
sudo systemctl restart postgresql
```

---

## 📧 Kontakt

W razie problemów:
- Email: office@nexait.pl
- GitHub Issues: https://github.com/TMMCx2/RampForge/issues

---

**Gratulacje! RampForge jest teraz w produkcji!** 🎉

Created by NEXAIT sp. z o.o. | https://nexait.pl/
