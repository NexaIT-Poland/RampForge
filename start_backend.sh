#!/bin/bash
# DCDock - Start Backend Server
# Simple script to start the backend API server

set -e  # Exit on error

echo "════════════════════════════════════════════════════════════════"
echo "  DCDock Backend Server"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Check if setup was run
if [ ! -d "backend/venv" ]; then
    echo "❌ Error: Virtual environment not found."
    echo "   Please run ./setup.sh first."
    exit 1
fi

# Navigate to backend directory
cd backend

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Check if database exists
if [ ! -f "dcdock.db" ]; then
    echo "Database not found. Initializing with demo data..."
    python -m app.seed
fi

# Start server
echo "Starting backend server on http://0.0.0.0:8000"
echo ""
echo "Press Ctrl+C to stop the server"
echo "════════════════════════════════════════════════════════════════"
echo ""

python run.py
