#!/bin/bash
# Build script for DCDock TUI client
# Creates portable executable for current platform

set -e

echo "🔨 Building DCDock TUI Client..."
echo ""

# Check if pyinstaller is installed
if ! command -v pyinstaller &> /dev/null; then
    echo "❌ PyInstaller not found. Installing..."
    pip install pyinstaller
fi

# Clean previous builds
echo "🧹 Cleaning previous builds..."
rm -rf build dist

# Build
echo "📦 Building executable..."
pyinstaller dcdock.spec

# Check result
if [ -f "dist/dcdock" ] || [ -f "dist/dcdock.exe" ] || [ -d "dist/dcdock.app" ]; then
    echo ""
    echo "✅ Build successful!"
    echo ""
    echo "Executable location:"

    if [ "$(uname)" == "Darwin" ]; then
        echo "  📁 dist/dcdock.app"
        echo ""
        echo "To run:"
        echo "  ./dist/dcdock.app/Contents/MacOS/dcdock"
    elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
        echo "  📁 dist/dcdock"
        echo ""
        echo "To run:"
        echo "  ./dist/dcdock"
    else
        echo "  📁 dist/dcdock.exe"
        echo ""
        echo "To run:"
        echo "  .\\dist\\dcdock.exe"
    fi

    echo ""
    echo "With server connection:"
    echo "  dcdock --api-url http://your-server:8000 --ws-url ws://your-server:8000"
else
    echo ""
    echo "❌ Build failed!"
    exit 1
fi
