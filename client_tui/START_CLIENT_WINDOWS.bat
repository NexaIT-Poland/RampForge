@echo off
REM ============================================================
REM  DCDock Client Launcher (Windows)
REM  Made by NEXAIT sp. z o.o.
REM ============================================================

cd /d "%~dp0"

echo.
echo ========================================
echo   DCDock Client v1.0.0
echo   Made by NEXAIT sp. z o.o.
echo ========================================
echo.

REM Check if venv exists
if not exist "venv\" (
    echo [!] Virtual environment not found!
    echo [*] Creating virtual environment...
    python -m venv venv
    if errorlevel 1 (
        echo [X] Failed to create virtual environment
        echo [!] Please install Python 3.11+ from python.org
        pause
        exit /b 1
    )
)

REM Activate venv
call venv\Scripts\activate.bat
if errorlevel 1 (
    echo [X] Failed to activate virtual environment
    pause
    exit /b 1
)

REM Check if dependencies are installed
python -c "import textual" 2>nul
if errorlevel 1 (
    echo [*] Installing dependencies...
    pip install --upgrade pip
    pip install -r requirements.txt
    if errorlevel 1 (
        echo [X] Failed to install dependencies
        pause
        exit /b 1
    )
)

REM Check if config.yaml exists
if not exist "config.yaml" (
    echo [!] config.yaml not found!
    if exist "config.yaml.example" (
        echo [*] Copying config.yaml.example to config.yaml
        copy config.yaml.example config.yaml
        echo.
        echo [!] Please edit config.yaml with your server details
        echo [!] Then run this script again
        pause
        exit /b 1
    ) else (
        echo [X] config.yaml.example not found!
        pause
        exit /b 1
    )
)

REM Start client
echo [*] Starting DCDock Client...
echo.
python -m app.main

REM Cleanup
deactivate
echo.
echo ========================================
echo   Client closed
echo ========================================
pause
