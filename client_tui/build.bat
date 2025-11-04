@echo off
REM Build script for DCDock TUI Client (Windows)
REM Creates portable executable

echo Building DCDock TUI Client...
echo.

REM Check if pyinstaller is installed
pyinstaller --version >nul 2>&1
if errorlevel 1 (
    echo PyInstaller not found. Installing...
    pip install pyinstaller
)

REM Clean previous builds
echo Cleaning previous builds...
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist

REM Build
echo Building executable...
pyinstaller dcdock.spec

REM Check result
if exist "dist\dcdock.exe" (
    echo.
    echo Build successful!
    echo.
    echo Executable location: dist\dcdock.exe
    echo.
    echo To run:
    echo   dist\dcdock.exe
    echo.
    echo With server connection:
    echo   dist\dcdock.exe --api-url http://your-server:8000 --ws-url ws://your-server:8000
) else (
    echo.
    echo Build failed!
    exit /b 1
)
