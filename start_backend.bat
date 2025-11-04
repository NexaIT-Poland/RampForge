@echo off
REM DCDock - Start Backend Server
REM Simple script to start the backend API server

setlocal

echo ================================================================
echo   DCDock Backend Server
echo ================================================================
echo.

REM Check if setup was run
if not exist "backend\venv\" (
    echo Error: Virtual environment not found.
    echo Please run setup.bat first.
    pause
    exit /b 1
)

REM Navigate to backend directory
cd backend

REM Activate virtual environment
echo Activating virtual environment...
call venv\Scripts\activate.bat

REM Check if database exists
if not exist "dcdock.db" (
    echo Database not found. Initializing with demo data...
    python -m app.seed
)

REM Start server
echo Starting backend server on http://0.0.0.0:8000
echo.
echo Press Ctrl+C to stop the server
echo ================================================================
echo.

python run.py
