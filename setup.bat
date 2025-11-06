@echo off
REM RampForge - Initial Setup Script for Windows
REM This script sets up both backend and client for first-time use

setlocal enabledelayedexpansion

echo ================================================================
echo   RampForge - Initial Setup
echo ================================================================
echo.

REM Check Python
where python >nul 2>&1
if %errorlevel% neq 0 (
    echo Error: Python is not installed or not in PATH.
    echo Please install Python 3.11 or higher from python.org
    pause
    exit /b 1
)

python --version
echo.

REM Setup Backend
echo ----------------------------------------------------------------
echo   Setting up Backend...
echo ----------------------------------------------------------------
cd backend

REM Create venv if it doesn't exist
if not exist "venv\" (
    echo Creating virtual environment...
    python -m venv venv
    echo Virtual environment created
) else (
    echo Virtual environment already exists
)

REM Activate venv
call venv\Scripts\activate.bat

REM Install dependencies
echo Installing dependencies...
python -m pip install --upgrade pip >nul 2>&1
pip install -e ".[dev]"
echo Dependencies installed

REM Fix bcrypt version
echo Ensuring compatible bcrypt version...
pip install --force-reinstall "bcrypt>=4.0.0,<5.0.0" >nul 2>&1
echo bcrypt version fixed

REM Install email-validator
echo Installing email-validator...
pip install email-validator >nul 2>&1
echo email-validator installed

REM Check if database exists
if not exist "rampforge.db" (
    echo Initializing database with demo data...
    python -m app.seed
    echo Database initialized
) else (
    echo Database already exists (skipping seed^)
)

call deactivate
cd ..
echo.

REM Setup Client TUI
echo ----------------------------------------------------------------
echo   Setting up Client TUI...
echo ----------------------------------------------------------------
cd client_tui

REM Create venv if it doesn't exist
if not exist "venv\" (
    echo Creating virtual environment...
    python -m venv venv
    echo Virtual environment created
) else (
    echo Virtual environment already exists
)

REM Activate venv
call venv\Scripts\activate.bat

REM Install dependencies
echo Installing dependencies...
python -m pip install --upgrade pip >nul 2>&1
pip install -e .
echo Dependencies installed

call deactivate
cd ..
echo.

REM Success message
echo ================================================================
echo   Setup Complete!
echo ================================================================
echo.
echo To start RampForge:
echo.
echo   1. Open Command Prompt 1 and run:
echo      start_backend.bat
echo.
echo   2. Open Command Prompt 2 and run:
echo      start_client.bat
echo.
echo Demo credentials:
echo   Admin:    admin@rampforge.com / admin123
echo   Operator: operator1@rampforge.com / operator123
echo.
echo ================================================================
pause
