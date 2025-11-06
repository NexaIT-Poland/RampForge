@echo off
REM RampForge - Start TUI Client
REM Simple script to start the terminal user interface client

setlocal

echo ================================================================
echo   RampForge TUI Client
echo ================================================================
echo.

REM Check if setup was run
if not exist "client_tui\venv\" (
    echo Error: Virtual environment not found.
    echo Please run setup.bat first.
    pause
    exit /b 1
)

REM Navigate to client directory
cd client_tui

REM Activate virtual environment
echo Activating virtual environment...
call venv\Scripts\activate.bat

REM Check if backend is running
echo Checking backend connection...
curl -s http://localhost:8000/docs >nul 2>&1
if %errorlevel% neq 0 (
    echo.
    echo Warning: Backend server not detected at http://localhost:8000
    echo Make sure to run start_backend.bat in another window first.
    echo.
    pause
)

REM Start client
echo Starting TUI client...
echo.
echo Demo credentials:
echo   Operator: operator1@rampforge.com / operator123
echo   Admin:    admin@rampforge.com / admin123
echo.
echo Press Ctrl+C to exit
echo ================================================================
echo.

python -m app.main
