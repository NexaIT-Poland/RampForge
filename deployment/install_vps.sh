#!/bin/bash
#
# RampForge VPS Installation Script
# For Ubuntu 22.04/24.04 LTS
#
# Usage: wget https://raw.githubusercontent.com/TMMCx2/RampForge/main/deployment/install_vps.sh
#        chmod +x install_vps.sh
#        sudo ./install_vps.sh
#

set -e

echo "=========================================="
echo "  RampForge VPS Installation"
echo "  Made by NEXAIT sp. z o.o."
echo "=========================================="
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then
   echo "❌ Please run as root (use sudo)"
   exit 1
fi

# Get configuration from user
read -p "Enter domain name (or press Enter to use IP only): " DOMAIN_NAME
read -p "Enter email for SSL certificate (required if using domain): " SSL_EMAIL
read -sp "Enter PostgreSQL password for rampforge_user: " DB_PASSWORD
echo ""
read -sp "Confirm PostgreSQL password: " DB_PASSWORD_CONFIRM
echo ""

if [ "$DB_PASSWORD" != "$DB_PASSWORD_CONFIRM" ]; then
    echo "❌ Passwords don't match!"
    exit 1
fi

echo ""
echo "📦 Installing system packages..."
apt update && apt upgrade -y
apt install -y git curl wget nano ufw python3 python3-pip python3-venv \
               postgresql postgresql-contrib nginx certbot python3-certbot-nginx

echo ""
echo "👤 Creating rampforge user..."
if ! id -u rampforge >/dev/null 2>&1; then
    useradd -m -s /bin/bash rampforge
    echo "✅ User rampforge created"
else
    echo "ℹ️  User rampforge already exists"
fi

echo ""
echo "🗄️  Configuring PostgreSQL..."
sudo -u postgres psql -c "CREATE DATABASE rampforge_prod;" || echo "ℹ️  Database already exists"
sudo -u postgres psql -c "CREATE USER rampforge_user WITH PASSWORD '$DB_PASSWORD';" || echo "ℹ️  User already exists"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE rampforge_prod TO rampforge_user;"
sudo -u postgres psql -d rampforge_prod -c "GRANT ALL ON SCHEMA public TO rampforge_user;"

# Configure PostgreSQL authentication
sed -i 's/local   all             all                                     peer/local   all             all                                     md5/' /etc/postgresql/*/main/pg_hba.conf
systemctl restart postgresql

echo ""
echo "📥 Cloning RampForge repository..."
cd /home/rampforge
if [ -d "RampForge" ]; then
    echo "ℹ️  Repository already exists, pulling latest..."
    cd RampForge
    sudo -u rampforge git pull
else
    sudo -u rampforge git clone https://github.com/TMMCx2/RampForge.git
    cd RampForge
fi

echo ""
echo "🐍 Setting up Python environment..."
cd /home/rampforge/RampForge/backend
sudo -u rampforge python3 -m venv venv
sudo -u rampforge venv/bin/pip install --upgrade pip
sudo -u rampforge venv/bin/pip install -r requirements.txt
sudo -u rampforge venv/bin/pip install gunicorn uvicorn[standard]

echo ""
echo "⚙️  Creating .env.production..."
SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(64))")

cat > /home/rampforge/RampForge/backend/.env.production <<EOF
DATABASE_URL=postgresql://rampforge_user:${DB_PASSWORD}@localhost:5432/rampforge_prod
SECRET_KEY=${SECRET_KEY}
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=480
ENVIRONMENT=production
BACKEND_CORS_ORIGINS=["https://${DOMAIN_NAME:-localhost}"]
HOST=0.0.0.0
PORT=8000
LOG_LEVEL=INFO
EOF

chown rampforge:rampforge /home/rampforge/RampForge/backend/.env.production
chmod 600 /home/rampforge/RampForge/backend/.env.production

echo ""
echo "📊 Initializing database..."
cd /home/rampforge/RampForge/backend
sudo -u rampforge bash -c "source venv/bin/activate && export \$(cat .env.production | xargs) && python3 -m app.seed"

echo ""
echo "📁 Creating log directory..."
mkdir -p /var/log/rampforge
chown rampforge:rampforge /var/log/rampforge

echo ""
echo "🔧 Creating systemd service..."
cat > /etc/systemd/system/rampforge.service <<'EOF'
[Unit]
Description=RampForge FastAPI Backend
After=network.target postgresql.service

[Service]
Type=notify
User=rampforge
Group=rampforge
WorkingDirectory=/home/rampforge/RampForge/backend
Environment="PATH=/home/rampforge/RampForge/backend/venv/bin"
EnvironmentFile=/home/rampforge/RampForge/backend/.env.production

ExecStart=/home/rampforge/RampForge/backend/venv/bin/gunicorn \
    --workers 4 \
    --worker-class uvicorn.workers.UvicornWorker \
    --bind 0.0.0.0:8000 \
    --timeout 120 \
    --access-logfile /var/log/rampforge/access.log \
    --error-logfile /var/log/rampforge/error.log \
    --log-level info \
    app.main:app

Restart=always
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable rampforge
systemctl start rampforge

echo ""
echo "🌐 Configuring Nginx..."

if [ -n "$DOMAIN_NAME" ]; then
    # Configuration with domain
    cat > /etc/nginx/sites-available/rampforge <<EOF
upstream rampforge_backend {
    server 127.0.0.1:8000;
}

server {
    listen 80;
    server_name ${DOMAIN_NAME};

    location /api/ {
        proxy_pass http://rampforge_backend;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }

    location /api/v1/ws {
        proxy_pass http://rampforge_backend;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_connect_timeout 7d;
        proxy_send_timeout 7d;
        proxy_read_timeout 7d;
    }

    location /health {
        proxy_pass http://rampforge_backend/api/v1/health;
        access_log off;
    }
}
EOF
else
    # Configuration with IP only
    VPS_IP=$(curl -s ifconfig.me)
    cat > /etc/nginx/sites-available/rampforge <<EOF
upstream rampforge_backend {
    server 127.0.0.1:8000;
}

server {
    listen 80;
    server_name ${VPS_IP};

    location /api/ {
        proxy_pass http://rampforge_backend;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
    }

    location /api/v1/ws {
        proxy_pass http://rampforge_backend;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_connect_timeout 7d;
        proxy_send_timeout 7d;
        proxy_read_timeout 7d;
    }

    location /health {
        proxy_pass http://rampforge_backend/api/v1/health;
        access_log off;
    }
}
EOF
fi

ln -sf /etc/nginx/sites-available/rampforge /etc/nginx/sites-enabled/
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl restart nginx

echo ""
echo "🔥 Configuring firewall..."
ufw --force enable
ufw allow 22/tcp
ufw allow 80/tcp
ufw allow 443/tcp

if [ -n "$DOMAIN_NAME" ] && [ -n "$SSL_EMAIL" ]; then
    echo ""
    echo "🔒 Setting up SSL certificate..."
    certbot --nginx -d $DOMAIN_NAME --non-interactive --agree-tos -m $SSL_EMAIL
fi

echo ""
echo "📦 Creating backup directory..."
mkdir -p /var/backups/rampforge
chown rampforge:rampforge /var/backups/rampforge

# Create backup script
cat > /home/rampforge/backup_db.sh <<EOF
#!/bin/bash
BACKUP_DIR="/var/backups/rampforge"
DB_NAME="rampforge_prod"
DB_USER="rampforge_user"
TIMESTAMP=\$(date +"%Y%m%d_%H%M%S")
BACKUP_FILE="\$BACKUP_DIR/rampforge_backup_\$TIMESTAMP.sql.gz"

export PGPASSWORD='${DB_PASSWORD}'
pg_dump -U \$DB_USER -h localhost \$DB_NAME | gzip > \$BACKUP_FILE
find \$BACKUP_DIR -name "rampforge_backup_*.sql.gz" -mtime +30 -delete
unset PGPASSWORD

echo "Backup created: \$BACKUP_FILE"
EOF

chmod +x /home/rampforge/backup_db.sh
chown rampforge:rampforge /home/rampforge/backup_db.sh

# Add to crontab
(crontab -u rampforge -l 2>/dev/null; echo "0 2 * * * /home/rampforge/backup_db.sh >> /var/log/rampforge/backup.log 2>&1") | crontab -u rampforge -

echo ""
echo "=========================================="
echo "✅ Installation Complete!"
echo "=========================================="
echo ""
echo "📋 Summary:"
echo "  - Backend running on port 8000"
echo "  - Nginx configured"
if [ -n "$DOMAIN_NAME" ]; then
    echo "  - Domain: https://${DOMAIN_NAME}"
else
    echo "  - Access via: http://$(curl -s ifconfig.me)"
fi
echo "  - Database: rampforge_prod"
echo "  - User: rampforge_user"
echo ""
echo "🔐 Default credentials:"
echo "  Admin: admin@rampforge.com / Admin123!@#"
echo "  Operator: operator1@rampforge.com / Operator123!@#"
echo ""
echo "⚠️  IMPORTANT: Change these passwords immediately!"
echo ""
echo "📊 Check status:"
echo "  sudo systemctl status rampforge"
echo "  sudo journalctl -u rampforge -f"
echo ""
echo "🧪 Test API:"
if [ -n "$DOMAIN_NAME" ]; then
    echo "  curl https://${DOMAIN_NAME}/api/v1/health"
else
    echo "  curl http://$(curl -s ifconfig.me)/api/v1/health"
fi
echo ""
echo "📚 Full documentation: /home/rampforge/RampForge/PRODUCTION_DEPLOYMENT_GUIDE.md"
echo ""
echo "Made by NEXAIT sp. z o.o. | office@nexait.pl | https://nexait.pl/"
echo ""
