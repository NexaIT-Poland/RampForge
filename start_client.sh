#!/bin/bash
# RampForge - Start TUI Client
# Simple script to start the terminal user interface client

set -e  # Exit on error

echo "════════════════════════════════════════════════════════════════"
echo "  RampForge TUI Client"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Check if setup was run
if [ ! -d "client_tui/venv" ]; then
    echo "❌ Error: Virtual environment not found."
    echo "   Please run ./setup.sh first."
    exit 1
fi

# Navigate to client directory
cd client_tui

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Check if backend is running
if ! curl -s http://localhost:8000/docs > /dev/null 2>&1; then
    echo "⚠️  Warning: Backend server not detected at http://localhost:8000"
    echo "   Make sure to run ./start_backend.sh in another terminal first."
    echo ""
    read -p "Press Enter to continue anyway, or Ctrl+C to exit..."
    echo ""
fi

# Start client
echo "Starting TUI client..."
echo ""
echo "Demo credentials (v1.0.0):"
echo "  Operator: operator1@rampforge.com / Operator123!@#"
echo "  Admin:    admin@rampforge.com / Admin123!@#"
echo ""
echo "Press Ctrl+C to exit"
echo "════════════════════════════════════════════════════════════════"
echo ""

python3 -m app.main
