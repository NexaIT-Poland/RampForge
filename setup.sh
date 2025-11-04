#!/bin/bash
# DCDock - Initial Setup Script
# This script sets up both backend and client for first-time use

set -e  # Exit on error

echo "════════════════════════════════════════════════════════════════"
echo "  DCDock - Initial Setup"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Check Python version
PYTHON_CMD=""
if command -v python3 &> /dev/null; then
    PYTHON_CMD="python3"
elif command -v python &> /dev/null; then
    PYTHON_CMD="python"
else
    echo "❌ Error: Python 3 is not installed."
    echo "   Please install Python 3.11 or higher."
    exit 1
fi

PYTHON_VERSION=$($PYTHON_CMD --version 2>&1 | awk '{print $2}')
echo "✓ Found Python: $PYTHON_VERSION"
echo ""

# Setup Backend
echo "────────────────────────────────────────────────────────────────"
echo "  Setting up Backend..."
echo "────────────────────────────────────────────────────────────────"
cd backend

# Create venv if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    $PYTHON_CMD -m venv venv
    echo "✓ Virtual environment created"
else
    echo "✓ Virtual environment already exists"
fi

# Activate venv
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip > /dev/null 2>&1
pip install -e ".[dev]"
echo "✓ Dependencies installed"

# Fix bcrypt version if needed
echo "Ensuring compatible bcrypt version..."
pip install --force-reinstall "bcrypt>=4.0.0,<5.0.0" > /dev/null 2>&1
echo "✓ bcrypt version fixed"

# Install email-validator
echo "Installing email-validator..."
pip install email-validator > /dev/null 2>&1
echo "✓ email-validator installed"

# Check if database exists
if [ ! -f "dcdock.db" ]; then
    echo "Initializing database with demo data..."
    python -m app.seed
    echo "✓ Database initialized"
else
    echo "✓ Database already exists (skipping seed)"
fi

deactivate
cd ..
echo ""

# Setup Client TUI
echo "────────────────────────────────────────────────────────────────"
echo "  Setting up Client TUI..."
echo "────────────────────────────────────────────────────────────────"
cd client_tui

# Create venv if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    $PYTHON_CMD -m venv venv
    echo "✓ Virtual environment created"
else
    echo "✓ Virtual environment already exists"
fi

# Activate venv
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip > /dev/null 2>&1
pip install -e .
echo "✓ Dependencies installed"

deactivate
cd ..
echo ""

# Success message
echo "════════════════════════════════════════════════════════════════"
echo "  ✓ Setup Complete!"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "To start DCDock:"
echo ""
echo "  1. Open Terminal 1 and run:"
echo "     ./start_backend.sh"
echo ""
echo "  2. Open Terminal 2 and run:"
echo "     ./start_client.sh"
echo ""
echo "Demo credentials:"
echo "  Admin:    admin@dcdock.com / admin123"
echo "  Operator: operator1@dcdock.com / operator123"
echo ""
echo "════════════════════════════════════════════════════════════════"
