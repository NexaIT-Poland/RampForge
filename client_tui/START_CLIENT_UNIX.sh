#!/bin/bash
# ============================================================
#  RampForge Client Launcher (Linux/Mac)
#  Made by NEXAIT sp. z o.o.
# ============================================================

cd "$(dirname "$0")"

echo ""
echo "========================================"
echo "  RampForge Client v1.0.0"
echo "  Made by NEXAIT sp. z o.o."
echo "========================================"
echo ""

# Check if venv exists
if [ ! -d "venv" ]; then
    echo "[!] Virtual environment not found!"
    echo "[*] Creating virtual environment..."
    python3 -m venv venv
    if [ $? -ne 0 ]; then
        echo "[X] Failed to create virtual environment"
        echo "[!] Please install Python 3.11+"
        read -p "Press Enter to exit..."
        exit 1
    fi
fi

# Activate venv
source venv/bin/activate
if [ $? -ne 0 ]; then
    echo "[X] Failed to activate virtual environment"
    read -p "Press Enter to exit..."
    exit 1
fi

# Check if dependencies are installed
python -c "import textual" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "[*] Installing dependencies..."
    pip install --upgrade pip
    pip install -r requirements.txt
    if [ $? -ne 0 ]; then
        echo "[X] Failed to install dependencies"
        read -p "Press Enter to exit..."
        exit 1
    fi
fi

# Check if config.yaml exists
if [ ! -f "config.yaml" ]; then
    echo "[!] config.yaml not found!"
    if [ -f "config.yaml.example" ]; then
        echo "[*] Copying config.yaml.example to config.yaml"
        cp config.yaml.example config.yaml
        echo ""
        echo "[!] Please edit config.yaml with your server details"
        echo "[!] Then run this script again"
        read -p "Press Enter to exit..."
        exit 1
    else
        echo "[X] config.yaml.example not found!"
        read -p "Press Enter to exit..."
        exit 1
    fi
fi

# Start client
echo "[*] Starting RampForge Client..."
echo ""
python -m app.main

# Cleanup
deactivate
echo ""
echo "========================================"
echo "  Client closed"
echo "========================================"
read -p "Press Enter to exit..."
